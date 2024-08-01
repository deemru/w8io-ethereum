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
    private $lastUp;
    private $height;
    private $txheight;
    private $lastTarget;
    private $q_getTxIdsFromTo;

    public function __construct( $db )
    {
        $this->ts = new Triples( $db, 'ts', 1, ['INTEGER PRIMARY KEY', 'INTEGER', 'TEXT'], [0, 1] );

        $this->db = $this->ts;
        $this->db->db->exec( 'PRAGMA journal_size_limit = ' . ( 8 * 1024 * 1024 ) );
        $this->db->db->exec( 'PRAGMA wal_autocheckpoint = ' . ( 8 * 1024 ) );

        $this->hs = new Triples( $this->db, 'hs', 1, ['INTEGER PRIMARY KEY', 'TEXT', 'INTEGER'] );
        $this->parser = new BlockchainParser( $this->db );

        $this->setHeight();
        $this->setTxHeight();
        $this->lastUp = 0;
    }

    private function ts2r( $key, $tx )
    {
        $txid = h2b( $tx['hash'] );
        $bucket = unpack( 'J1', $txid )[1];
        return [ $key, $bucket, substr( $txid, 8 ) ];
    }

    public function getTransactionId( $key )
    {
        $r = $this->ts->getUno( 0, $key );
        if( $r === false )
            return false;

        return e58( pack( 'J', $r[1] ) . $r[2] );
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

    public function getTxIdsAtHeight( $height )
    {
        $from = w8h2k( $height );
        $to = w8h2kg( $height );
        return $this->getTxIdsFromTo( $from, $to );
    }

    public function getTxIdsFromTo( $from, $to )
    {
        if( !isset( $this->q_getTxIdsFromTo ) )
        {
            $this->q_getTxIdsFromTo = $this->ts->db->prepare( 'SELECT * FROM ts WHERE r0 >= ? AND r0 <= ? ORDER BY r0 ASC' );
            if( $this->q_getTxIdsFromTo === false )
                w8_err();
        }

        if( false === $this->q_getTxIdsFromTo->execute( [ $from, $to ] ) )
            w8_err();

        $txids = [];
        foreach( $this->q_getTxIdsFromTo as $r )
            $txids[$r[0]] = e58( pack( 'J', $r[1] ) . $r[2] );

        return $txids;
    }

    public function rollback( $from )
    {
        if( $this->height - $from > 1000 )
            $this->rollback( $from + 1000 );

        $txfrom = w8h2k( $from ) - 1; // all txs + last generator
        $tt = microtime( true );
        $this->db->begin();
        {
            $this->parser->rollback( $txfrom );
            $this->hs->query( 'DELETE FROM hs WHERE r0 >= ' . $from );
            $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $txfrom );
        }
        $this->db->commit();
        wk()->log( 'i', $this->height . ' >> ' . ( $from - 1 ) . ' (rollback) (' . (int)( 1000 * ( microtime( true ) - $tt ) ) . ' ms)' );

        $this->setHeight( $from - 1 );
        $this->setTxHeight();
    }

    private function fixate( $fixate )
    {
        $this->parser->rollback( $fixate );
        $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $fixate );

        $height = w8k2h( $fixate );
        $txfrom = w8k2i( $this->txheight ) + 1;
        $fixate = w8k2i( $fixate );
        wk()->log( 'i', $height . ' (' . $txfrom . ' >> ' . $fixate . ') (fixate)' );

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

    public function getBlockMinimal( $number ) : array|false
    {
        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x' . dechex( $number ) . '",false],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getBlock( $number ) : array|false
    {
        $local = localBlocks()->getValueByKey( $number );
        if( 0 && $local !== false )
        {
            localBlocks()->reset();
            return $local;
        }

        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x' . dechex( $number ) . '",true],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getBlockTraces( $number ) : array|false
    {
        $local = localTraces()->getValueByKey( $number );
        if( 0 && $local !== false )
        {
            localTraces()->reset();
            return $local;
        }

        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"trace_replayBlockTransactions","params":["0x' . dechex( $number ) . '",["trace","stateDiff"]],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function getBlockReceipts( $number ) : array|false
    {
        $local = localTraces()->getValueByKey( $number );
        if( 0 && $local !== false )
        {
            localTraces()->reset();
            return $local;
        }

        $json = wk()->fetch( '/', true, '{"jsonrpc":"2.0","method":"eth_getBlockReceipts","params":["0x' . dechex( $number ) . '"],"id":1}' );
        if( $json === false || false === ( $json = jd( $json ) ) || !isset( $json['result'] ) )
            return false;

        return $json['result'];
    }

    public function update( $block = null )
    {
        $entrance = microtime( true );

        //$this->rollback( 590000 );

        $from = $this->height;
        $height = $this->lastTarget ?? -1;
        if( $height - $from - W8IO_MAX_UPDATE_BATCH < 0 )
        {
            if( false === ( $height = $this->getHeight() ) )
            {
                wk()->log( 'w', 'OFFLINE: cannot get last header' );
                return W8IO_STATUS_OFFLINE;
            }
            else
            {
                $this->lastTarget = $height;
            }
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
            $block = $this->getBlockMinimal( $i );
            $block = $this->getBlock( $i );
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
                    $rollback = true;
                    $fixate = w8h2k( $from );
                }

                break;
            }

            wk()->log( 'w', 'fork @ ' . $i );
            $i--;
        }

        $newHdrs = [];
        $newTxs = [];
        $txCount = 0;

        $to = min( $height, $from + W8IO_MAX_UPDATE_BATCH - 1 );
        //$to = min( $height, $from + 1 - 1 );
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
            $txs = $block['transactions'];
            $n = count( $txs );
            if( $n )
            {
                $baseFee = $block['baseFeePerGas'];
                $traces = $this->getBlockTraces( $i );
                $receipts = $this->getBlockReceipts( $i );
                $key = w8h2k( $i );
                for( $j = 0; $j < $n; ++$j, ++$key )
                {
                    $tx = $txs[$j];
                    $hash = $tx['hash'];
                    $txTrace = $traces[$j];
                    $txReceipt = $receipts[$j];//localReceipts()->getValueByKey( $hash );
                    //localReceipts()->reset();
                    if( $hash !== $txTrace['transactionHash'] ||
                        $hash !== $txReceipt['transactionHash'] ||
                        $blockHash !== $txReceipt['blockHash'] ||
                        $i !== intval( $txReceipt['blockNumber'], 16 ) ||
                        $j !== intval( $txReceipt['transactionIndex'], 16 ) )
                    {
                        wk()->log( 'w', 'on-the-fly transaction change @ ' . $i );
                        return W8IO_STATUS_WARNING;
                    }

                    $tx['trace'] = $txTrace;
                    $tx['receipt'] = $txReceipt;
                    $tx['baseFee'] = $baseFee;
                    $newTxs[$key] = $tx;
                    $txheight = $key;
                }
            }

            if( !isset( $fixate ) )
                $fixate = w8h2k( $i, $n );

            unset( $block['transactions'] );
            $reference = $blockHash;
            $newHdrs[$i] = $block;

            if( 0 && $i > $from )
            {
                wk()->log( ( $i - 1 ) . ' (' . $txCount . ')' );
                $txCount = 0;
            }

            $txCount += $n;
        }

        if( 0 === count( $newHdrs ) )
            return W8IO_STATUS_NORMAL;

        $this->db->begin();
        {
            if( isset( $rollback ) )
                $this->rollback( $from );
            else if( $this->txheight >= $fixate )
            {
                $this->fixate( $fixate );
                $txCount -= w8k2i( $fixate );
            }

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

        $this->lastUp = microtime( true );
        $newTxs = $from === $to ? ( ' +' . count( $newTxs ) ) : '';
        $ram = memory_get_usage( true ) / 1024 / 1024;
        $ram = sprintf( '%.00f MiB', $ram );
        wk()->log( 's', $to . ' (' . $txCount . ')' . $newTxs . ' (' . (int)( ( $this->lastUp - $entrance ) * 1000 ) . ' ms) (' . $ram . ')' );
        return W8IO_STATUS_UPDATED;
    }
}
