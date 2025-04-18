<?php

namespace w8io;

use deemru\Triples;
use deemru\KV;

define( 'W8IO_STATUS_WARNING', -2 );
define( 'W8IO_STATUS_OFFLINE', -1 );
define( 'W8IO_STATUS_NORMAL', 0 );
define( 'W8IO_STATUS_UPDATED', 1 );

class Blockchain
{
    public Triples $ts;
    public Triples $hs;
    public BlockchainParser $parser;

    private Triples $db;
    private Triples $cacheDB;
    private KV $kvBlocks;
    private KV $kvTxs;
    private $lastUp;
    private $height;
    private $txheight;
    private $lastTarget;
    private $mainChainId;
    private $mainChainHeight;
    private $syncingHeight;

    public function __construct( $db )
    {
        $this->ts = new Triples( $db, 'ts', 1, ['INTEGER PRIMARY KEY', 'INTEGER', 'TEXT'], [0, 1] );

        $this->db = $this->ts;
        $this->db->db->exec( 'PRAGMA journal_size_limit = ' . ( 8 * 1024 * 1024 ) );
        $this->db->db->exec( 'PRAGMA wal_autocheckpoint = ' . ( 8 * 1024 ) );

        $this->hs = new Triples( $this->db, 'hs', 1, ['INTEGER PRIMARY KEY', 'TEXT', 'INTEGER'] );
        $this->parser = new BlockchainParser( $this->db );

        $this->cacheDB = new Triples( W8IO_CACHE_DB );
        $this->kvBlocks = ( new KV( false ) )->setStorage( $this->cacheDB, 'blocks', true, 'INTEGER PRIMARY KEY', 'TEXT' )->setValueAdapter( function( $value ){ return jzd( $value ); }, function( $value ){ return jze( $value ); } );
        $this->kvTxs = ( new KV( false ) )->setStorage( $this->cacheDB, 'txs', true, 'TEXT UNIQUE', 'TEXT' )->setValueAdapter( function( $value ){ return jzd( $value ); }, function( $value ){ return jze( $value ); } );

        $this->setHeight();
        $this->setTxHeight();
        $this->lastUp = 0;
        $this->mainChainId = wkn()->getData( "mainChainId", W8IO_L1_CONTRACT );
        if( $this->mainChainId === false )
            $this->mainChainId = 0;
        $this->mainChainHeight = 0;
        $this->syncingHeight = 0;
    }

    private function ts2r( $key, $tx )
    {
        $txid = h2b( $tx['hash'] );
        $bucket = unpack( 'J1', $txid )[1];
        return [ $key, $bucket, substr( $txid, 8 ) ];
    }

    public function height()
    {
        return $this->height;
    }

    private function setHeight( $height = null )
    {
        if( !isset( $height ) )
        {
            $height = $this->hs->getHigh( 0 );
            if( $height === false )
                $height = -1;
        }

        $this->height = $height;
    }

    private function setTxHeight( $txheight = null )
    {
        if( !isset( $txheight ) )
        {
            $txheight = $this->ts->getHigh( 0 );
            if( $txheight === false )
                $txheight = 0;
        }

        $hi = w8h2kg( $this->height - 1);
        if( $txheight < $hi )
            $txheight = $hi;

        $this->txheight = $txheight;
    }

    public function getMyUniqueAt( $at )
    {
        $q = $this->hs->getUno( 0, $at );
        if( !isset( $q[1]) )
            return false;

        return b2h( $q[1] );
    }

    public function rollback( $from )
    {
        if( $this->height - $from > 1000 )
            $this->rollback( $from + 1000 );

        $txfrom = w8h2k( $from );
        $tt = microtime( true );
        $this->db->begin();
        {
            $this->parser->rollback( $txfrom );
            $this->hs->query( 'DELETE FROM hs WHERE r0 >= ' . $from );
            $this->kvBlocks->db->query( 'DELETE FROM blocks WHERE r0 >= ' . $from );
            $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $txfrom );
        }
        $this->db->commit();
        wk()->log( 'i', $this->height . ' >> ' . ( $from - 1 ) . ' (rollback) (' . (int)( 1000 * ( microtime( true ) - $tt ) ) . ' ms)' );

        $this->setHeight( $from - 1 );
        $this->setTxHeight();
    }

    private function blockUnique( $header )
    {
        return $header['hash'];
    }

    public function getHeight() : int|false
    {
        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return intval( $json['result'], 16 );
    }

    public function getBlock( $number, $cached = true ) : array|false
    {
        if( $cached )
        {
            $local = $this->kvBlocks->db->getUno( 0, $number );
            if( $local !== false )
            {
                $local = jzd( $local[1] );
                $local['cached'] = true;
                return $local;
            }
        }

        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x' . dechex( $number ) . '",false],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getOtherBlockByNumber( $number ) : array|false
    {
        $json = wkr()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x' . dechex( $number ) . '",true],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getOtherBlockByHash( $hash ) : array|false
    {
        $json = wkr()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params":["' . $hash . '",true],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getBlockByHash( $hash ) : array|false
    {
        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params":["' . $hash . '",false],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getRawTransactionByHash( $hash ) : string|false
    {
        $json = wkr()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getRawTransactionByHash","params":["' . $hash . '"],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getBlockTraces( $number ) : array|false
    {
        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"trace_replayBlockTransactions","params":["0x' . dechex( $number ) . '",["trace","stateDiff"]],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getBlockReceipts( $number ) : array|false
    {
        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockReceipts","params":["0x' . dechex( $number ) . '"],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getBlockTrace( $number )
    {
        $json = wk()->fetch( '/', true, $this->traceRequest( $number ) );
        if( $json === false || false === ( $json = jd( $json ) ) )
            return false;

        $block = $json[0]['result'] ?? false;
        $traces = $json[1]['result'] ?? false;
        $receipts = $json[2]['result'] ?? false;
        $diffs = $json[3]['result'] ?? false;

        if( $block === false || $traces === false || $receipts === false || $diffs === false )
            return false;

        return [ $block, $traces, $receipts, $diffs ];
    }

    private $cacheClient;
    private $cacheTo;
    private $cacheIterator;
    private $cacheBlocks = [];
    private $cacheTxs = [];

    private function cacheSync()
    {
        if( count( $this->cacheBlocks ) )
        {
            //wk()->log( 'cacheSync: +' . count( $this->cacheBlocks ) . ' (' . count( $this->cacheTxs ) . ')' );
            $this->cacheDB->begin();
            {
                $this->kvBlocks->db->merge( $this->cacheBlocks );
                $this->cacheBlocks = [];
            }
            if( count( $this->cacheTxs ) )
            {
                $this->kvTxs->db->merge( $this->cacheTxs );
                $this->cacheTxs = [];
            }
            $this->cacheDB->commit();
        }
    }

    private function cacheNext()
    {
        if( ++$this->cacheIterator <= $this->cacheTo )
            return $this->cacheIterator;
        return false;
    }

    private function cacheFill( $from, $count )
    {
        if( !isset( $this->cacheClient ) )
            $this->cacheClient = ( new \React\Http\Browser )->withTimeout( W8IO_RPC_API_TIMEOUT )->withHeader( 'Connection', 'close' );

        $this->cacheTo = $from + $count;
        $this->cacheIterator = $from;
        for( $i = 0; $i < W8IO_RPC_API_CONCURENCY; ++$i )
            $this->cacheStep();

        \React\EventLoop\Loop::run();
        $this->cacheSync();
    }

    function cacheRetry( $delay, $callback )
    {
        \React\EventLoop\Loop::get()->addTimer( $delay, $callback );
    }

    private function traceRequest( $number )
    {
        $hexnumber = dechex( $number );
        return
'[
    {"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x' . $hexnumber . '",true],"id":0},
    {"jsonrpc":"2.0","method":"debug_traceBlockByNumber","params":["0x' . $hexnumber . '",{"tracer":"callTracer"}],"id":1},
    {"jsonrpc":"2.0","method":"eth_getBlockReceipts","params":["0x' . $hexnumber . '"],"id":2},
    {"jsonrpc":"2.0","method":"debug_traceBlockByNumber","params":["0x' . $hexnumber . '",{"tracer":"prestateTracer","tracerConfig":{"diffMode":true}}],"id":3}
]';
    }

    private function simpleRequest( $number )
    {
        $hexnumber = dechex( $number );
        return
'[
    {"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x' . $hexnumber . '",false],"id":0}
]';
    }

    private function cacheStep( $number = false )
    {
        if( $number === false )
        {
            for( ;; )
            {
                $number = $this->cacheNext();
                if( $number === false )
                    return;
                $local = $this->kvBlocks->db->query( 'SELECT 1 FROM blocks WHERE r0 = ' . $number )->fetchColumn();
                if( $local === false )
                    break;
            }
        }

        ( $this->cacheClient->post( wk()->getNodeAddress(), [], $this->simpleRequest( $number ) ) )->then(
            function ( \Psr\Http\Message\ResponseInterface $response ) use ( $number )
            {
                $body = (string)$response->getBody();
                $json = jd( $body );

                $block = $json[0]['result'] ?? false;
                if( $block === false )
                    w8_err( 'cacheStep( ' . $number . ' ): unexpected response' );

                $hashes = $block['transactions'];
                $n = count( $hashes );

                // SIMPLE
                if( $n === 0 )
                {
                    $this->cacheBlocks[] = [ $number, jze( $block ) ];

                    if( count( $this->cacheBlocks ) >= 100 )
                        $this->cacheSync();

                    //wk()->log( 'cacheStep( ' . $number . ' ) +' . $n );
                    return $this->cacheStep();
                }

                // TRACE
                ( $this->cacheClient->post( wk()->getNodeAddress(), [], $this->traceRequest( $number ) ) )->then(
                    function ( \Psr\Http\Message\ResponseInterface $response ) use ( $number )
                    {
                        $body = (string)$response->getBody();
                        $json = jd( $body );

                        $block = $json[0]['result'] ?? false;
                        $traces = $json[1]['result'] ?? false;
                        $receipts = $json[2]['result'] ?? false;

                        if( $block === false || $traces === false || $receipts === false )
                            w8_err( 'cacheStep( ' . $number . ' ): unexpected response' );

                        $i = $block['number'];
                        $blockHash = $block['hash'];
                        $baseFee = $block['baseFeePerGas'];
                        $txs = $block['transactions'];
                        $n = count( $txs );

                        $hashes = [];
                        for( $j = 0; $j < $n; ++$j )
                        {
                            $tx = $txs[$j];
                            $receipt = $receipts[$j];
                            $trace = $traces[$j];

                            $hash = $tx['hash'];
                            $hashes[] = $hash;
                            if( $hash !== $trace['transactionHash'] ||
                                $hash !== $receipt['transactionHash'] ||
                                $blockHash !== $receipt['blockHash'] ||
                                $i !== $receipt['blockNumber'] ||
                                $j !== intval( $receipt['transactionIndex'], 16 ) )
                            {
                                w8_err( 'cacheStep( ' . $number . ' ): unexpected correlations' );
                            }

                            $tx['baseFee'] = $baseFee;
                            $tx['receipt'] = $receipt;
                            $tx['trace'] = $trace;

                            $this->cacheTxs[] = [ h2b( $hash ), jze( $tx ) ];
                        }

                        $block['transactions'] = $hashes;
                        $this->cacheBlocks[] = [ $number, jze( $block ) ];

                        if( count( $this->cacheBlocks ) >= 100 )
                            $this->cacheSync();

                        //wk()->log( 'cacheStep( ' . $number . ' ) +' . $n );
                        return $this->cacheStep();
                    },
                    function( \Exception $e ) use ( $number )
                    {
                        wk()->log( 'e', 'cacheStep( ' . $number . ' ): ' . $e->getCode() . ': ' . $e->getMessage() );
                        $this->cacheRetry( W8IO_OFFLINE_DELAY, function() use ( $number ){ $this->cacheStep( $number ); } );
                    }
                );
            },
            function( \Exception $e ) use ( $number )
            {
                wk()->log( 'e', 'cacheStep( ' . $number . ' ): ' . $e->getCode() . ': ' . $e->getMessage() );
                $this->cacheRetry( W8IO_OFFLINE_DELAY, function() use ( $number ){ $this->cacheStep( $number ); } );
            }
        );
    }

    private function base64UrlEncode( $data )
    {
        return rtrim( strtr( base64_encode( $data ), '+/', '-_'), '=' );
    }

    private function jwtheaders()
    {
        static $header = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9'; // $this->base64UrlEncode( json_encode( [ 'typ' => 'JWT', 'alg' => 'HS256' ] ) );
        $payload = $this->base64UrlEncode( json_encode( [ 'iat' => time() ] ) );
        $signature = $this->base64UrlEncode( hash_hmac( 'sha256', $header . '.' . $payload, W8IO_LOCAL_JWTSECRET, true ) );
        return
        [
            'Authorization: Bearer ' . $header . '.' . $payload . '.' . $signature,
            'Content-Type: application/json',
            'Accept: application/json'
        ];
    }

    public function followBlock( $block, $transactions, $finalized )
    {
        $payload =
        [
            'parentHash' => $block['parentHash'],
            'feeRecipient' => $block['miner'],
            'stateRoot' => $block['stateRoot'],
            'receiptsRoot' => $block['receiptsRoot'],
            'logsBloom' => $block['logsBloom'],
            'prevRandao' => $block['mixHash'],
            'blockNumber' => $block['number'],
            'gasLimit' => $block['gasLimit'],
            'gasUsed' => $block['gasUsed'],
            'timestamp' => $block['timestamp'],
            'extraData' => $block['extraData'],
            'baseFeePerGas' => $block['baseFeePerGas'],
            'blockHash' => $block['hash'],
            'transactions' => $transactions,
            'withdrawals' => $block['withdrawals'],
            'blobGasUsed' => $block['blobGasUsed'],
            'excessBlobGas' => $block['excessBlobGas'],
        ];

        $json = wke()->fetch( '/', true,
        '[
            {"jsonrpc":"2.0","method":"engine_newPayloadV3","params":[' . json_encode( $payload ) . ',[],"'.$block['parentBeaconBlockRoot'].'"],"id":1},
            {"jsonrpc":"2.0","method":"engine_forkchoiceUpdatedV3","params":[{"headBlockHash":"' . $block['hash'] . '","safeBlockHash":"' . $finalized . '","finalizedBlockHash":"' . $finalized . '"},null],"id":2}
        ]', null, $this->jwtheaders() );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json[0]['result'] ) || !isset( $json[1]['result'] ) )
            return false;

        return true;
    }

    public function advance( $hash, $finalized )
    {
        $json = wke()->fetch( '/', true, '{"jsonrpc":"2.0","method":"engine_forkchoiceUpdatedV3","params":[{"headBlockHash":"' . $hash . '","safeBlockHash":"' . $hash . '","finalizedBlockHash":"' . $finalized . '"},null],"id":1}', null, $this->jwtheaders() );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return true;
    }

    public function syncing()
    {
        $json = wke()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}', null, $this->jwtheaders() );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    private function followChain( $block, $finalized )
    {
        $transactions = [];
        foreach( $block['transactions'] as $tx )
        {
            if( !empty( $tx['accessList'] ) )
                exit( 'accessList at ' . $block['hash'] );
            $transactions[] = uk()->txRLP( $tx );
        }
        return $this->followBlock( $block, $transactions, $finalized );
    }

    public function update( $block = null )
    {
        $entrance = microtime( true );

        $from = $this->height;
        $height = $this->lastTarget ?? -1;
        if( $height - $from - W8IO_MAX_UPDATE_BATCH < 0 )
        {
            if( false === ( $height = $this->getHeight() ) )
            {
                wk()->log( 'w', 'OFFLINE: cannot getHeight' );
                return W8IO_STATUS_OFFLINE;
            }
            else
            {
                if( $this->lastTarget > $height )
                    wk()->log( 'w', 'height = ' . $height );
                $this->lastTarget = $height;

                static $checkOnce = true;
                if( $checkOnce && $height > W8IO_MAX_HISTORY_BATCH )
                {
                    if( false === $this->getBlockTrace( $height - W8IO_MAX_HISTORY_BATCH ) )
                        w8_err( 'getBlockTrace past W8IO_MAX_HISTORY_BATCH' );
                    $checkOnce = false;
                }
            }
        }

        if( $from >= $height )
        {
            if( !defined( 'W8IO_LOCAL_ENGINE' ) ) // do not use self advance
                return W8IO_STATUS_NORMAL;

            if( $this->mainChainHeight - $from - W8IO_MAX_HISTORY_BATCH < 0 )
            {
                $data = wkn()->fetch( '/addresses/data/' . W8IO_L1_CONTRACT, true, '{"keys":["chain_' . str_pad( strval( $this->mainChainId ), 8, '0', STR_PAD_LEFT ) . '","finalizedBlock","mainChainId"]}' );
                if( $data === false || false === ( $data = jd( $data ) ) )
                {
                    wk()->log( 'w', 'OFFLINE: cannot get chain data' );
                    return W8IO_STATUS_OFFLINE;
                }

                $mainChainId = $data[2]['value'] ?? 0;
                if( $mainChainId !== $this->mainChainId )
                {
                    $this->mainChainId = $mainChainId;
                    wk()->log( 'w', 'SWITCHING to mainChainId = ' . $this->mainChainId );
                    return W8IO_STATUS_UPDATED;
                }

                [ $height, $head ] = explode( ',', $data[0]['value'] );
                $this->mainChainHeight = (int)$height;

                if( $from >= $this->mainChainHeight )
                    return W8IO_STATUS_NORMAL;
            }

            if( $this->mainChainHeight - $from - W8IO_MAX_HISTORY_BATCH < 0 )
            {
                $targetHeight = $this->mainChainHeight;
                $targetBlock = $this->getOtherBlockByHash( '0x' . $head );
                if( $targetBlock === false )
                {
                    wk()->log( 'w', 'OFFLINE: getOtherBlockByHash() bad response' );
                    return W8IO_STATUS_OFFLINE;
                }

                $finalized = '0x' . $data[1]['value'];
                $this->followChain( $targetBlock, $finalized );
                $this->syncingHeight = $targetHeight;
            }
            else
            if( $this->syncingHeight - $from - W8IO_MAX_UPDATE_BATCH < 0 )
            {
                $targetHeight = $from + W8IO_MAX_HISTORY_BATCH;
                $targetBlock = $this->getOtherBlockByNumber( $targetHeight );
                if( $targetBlock === false )
                {
                    wk()->log( 'w', 'OFFLINE: getOtherBlockByNumber() bad response' );
                    return W8IO_STATUS_OFFLINE;
                }

                $finalized = $targetBlock['hash'];
                $this->followChain( $targetBlock, $finalized );
                $this->syncingHeight = $targetHeight;
            }

            for( ;; )
            {
                $height = $this->getHeight();
                if( $height === false )
                {
                    wk()->log( 'w', 'OFFLINE: syncing getHeight' );
                    return W8IO_STATUS_OFFLINE;
                }
                if( $height > $from )
                    break;
                if( microtime( true ) - $entrance > W8IO_OFFLINE_DELAY )
                {
                    $this->syncingHeight = 0;
                    break;
                }
                usleep( 50000 );
            }

            return W8IO_STATUS_UPDATED;
        }

        $cached = false;
        if( $from + W8IO_MAX_UPDATE_BATCH < $height )
        {
            $cached = true;
            if( W8IO_RPC_API_CONCURENCY > 1 )
                $this->cacheFill( $from, W8IO_MAX_UPDATE_BATCH );
        }

        if( -1 === $from )
        {
            $blockHeight = -1;
            $i = $from = 0;
            $reference = '0x0000000000000000000000000000000000000000000000000000000000000000';
            wk()->log( 'w', 'starting from GENESIS' );
        }
        else
        for( $i = $from + 1;; )
        {
            $block = $this->getBlock( $i, $cached );
            if( $block === false )
            {
                wk()->log( 'w', 'OFFLINE: cannot get block' );
                return W8IO_STATUS_OFFLINE;
            }
            $blockHeight = $i;

            $reference = $this->getMyUniqueAt( $i - 1 );

            // STABLE BLOCK
            if( $reference === $block['parentHash'] )
            {
                wk()->log( 'd', 'stable @ ' . ( $i - 1 ) );

                $from = $i;
                if( $this->height >= $from )
                {
                    $this->rollback( $from );
                    return W8IO_STATUS_UPDATED;
                }

                break;
            }

            wk()->log( 'w', 'fork @ ' . $i );
            $cached = false;
            $i--;
        }

        $newHdrs = [];
        $newTxs = [];
        $txCount = 0;
        $cacheBlocks = [];
        $cacheTxs = [];
        $cacheCount = 0;

        $to = min( $height, $from + W8IO_MAX_UPDATE_BATCH - 1 );
        for( /* $i = $from */; $i <= $to; $i++ )
        {
            if( $blockHeight !== $i )
            {
                $block = $this->getBlock( $i );
                if( $block === false )
                {
                    wk()->log( 'w', 'OFFLINE: cannot get block' );
                    return W8IO_STATUS_OFFLINE;
                }
                $blockHeight = $i;
            }

            if( $reference !== $block['parentHash'] )
            {
                wk()->log( 'w', 'on-the-fly change @ ' . $i );
                return W8IO_STATUS_WARNING;
            }

            $blockHash = $block['hash'];
            $hashes = $block['transactions'];
            $n = count( $hashes );
            if( $block['cached'] ?? false )
            {
                ++$cacheCount;
                if( $n )
                {
                    $key = w8h2k( $i );
                    foreach( $hashes as $hash )
                    {
                        $tx = $this->kvTxs->getValueByKey( h2b( $hash ) );
                        if( $tx === false )
                            w8_err();
                        $this->kvTxs->reset();
                        $newTxs[$key++] = $tx;
                    }
                    $txheight = $key - 1;
                }
            }
            else
            {
                if( $n )
                {
                    $key = w8h2k( $i );
                    $result = $this->getBlockTrace( $i );
                    if( $result === false )
                    {
                        wk()->log( 'w', 'OFFLINE: cannot get block trace' );
                        return W8IO_STATUS_OFFLINE;
                    }
                    [ $block, $traces, $receipts, $diffs ] = $result;
                    $txs = $block['transactions'];
                    $baseFee = $block['baseFeePerGas'];

                    for( $j = 0; $j < $n; ++$j )
                    {
                        $tx = $txs[$j];
                        $receipt = $receipts[$j] ?? false;
                        $trace = $traces[$j] ?? false;
                        $hash = $hashes[$j];
                        $diff = $diffs[$j];

                        if( $receipt === false )
                        {
                            wk()->log( 'w', 'OFFLINE: no receipt at ' . $i . ' for tx ' . $j . '/' . $n );
                            return W8IO_STATUS_OFFLINE;
                        }

                        if( $trace === false )
                        {
                            wk()->log( 'w', 'OFFLINE: no trace at ' . $i . ' for tx ' . $j . '/' . $n );
                            return W8IO_STATUS_OFFLINE;
                        }

                        if( $diff === false )
                        {
                            wk()->log( 'w', 'OFFLINE: no diff at ' . $i . ' for tx ' . $j . '/' . $n );
                            return W8IO_STATUS_OFFLINE;
                        }

                        if( $hash !== $tx['hash'] ||
                            $hash !== $trace['txHash'] ||
                            $hash !== $diff['txHash'] ||
                            $hash !== $receipt['transactionHash'] ||
                            $blockHash !== $receipt['blockHash'] )
                        {
                            wk()->log( 'w', 'on-the-fly transaction change @ ' . $i );
                            return W8IO_STATUS_WARNING;
                        }

                        $tx['baseFee'] = $baseFee;
                        $tx['receipt'] = $receipt;
                        $tx['trace'] = $trace['result'];
                        $tx['diff'] = $diff['result'];

                        $newTxs[$key++] = $tx;
                        $cacheTxs[] = [ h2b( $hash ), jze( $tx ) ];
                    }
                    $txheight = $key - 1;
                    $block['transactions'] = $hashes;
                }

                $cacheBlocks[] = [ $i, jze( $block ) ];
            }

            $reference = $blockHash;
            $newHdrs[$i] = $block;
            $txCount += $n;
        }

        $this->db->begin();
        {
            if( isset( $rollback ) )
                $this->rollback( $from );

            $parserTxs = $newTxs;

            $hs = [];
            foreach( $newHdrs as $height => $block )
            {
                $hs[] = [ $height, h2b( $this->blockUnique( $block ) ), intval( $block['timestamp'], 16 ) ];
                $parserTxs[w8h2kg( $height )] = [ 'type' => TX_MINER, 'block' => $block ];
            }
            $this->hs->merge( $hs );

            if( count( $newTxs ) )
            {
                $ts = [];
                foreach( $newTxs as $key => $tx )
                    $ts[] = $this->ts2r( $key, $tx );
                $this->ts->merge( $ts );
            }

            ksort( $parserTxs );
            $this->parser->update( $parserTxs );

            $this->setHeight( $height );
            if( count( $newTxs ) )
                $this->setTxHeight( $txheight );
            else
                $this->setTxHeight( $this->txheight );
        }
        $this->db->commit();

        if( count( $cacheBlocks ) )
        {
            $this->cacheDB->begin();
            $this->kvBlocks->db->merge( $cacheBlocks );
            if( count( $cacheTxs ) )
                $this->kvTxs->db->merge( $cacheTxs );
            $this->cacheDB->commit();
        }

        $this->lastUp = microtime( true );
        $ram = memory_get_usage( true ) / 1024 / 1024;
        $ram = sprintf( '%.00f MiB', $ram );
        $cacheCount = $cacheCount ? ( ' (' . $cacheCount . ' cached)' ) : '';
        wk()->log( 's', ( $from - 1 ) . ' -> ' . $to . str_pad( ' +' . $txCount, 7, ' ', STR_PAD_LEFT ) . ' txs in ' . (int)( ( $this->lastUp - $entrance ) * 1000 ) . ' ms' . $cacheCount . ' (' . $ram . ')' );
        return W8IO_STATUS_UPDATED;
    }
}
