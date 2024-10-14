<?php

namespace w8io;

require_once 'common.php';

use deemru\Pairs;
use deemru\Triples;
use deemru\KV;

class BlockchainParser
{
    public Triples $db;
    public Triples $pts;
    public KV $kvAddresses;
    public KV $kvContracts;
    public KV $kvAliasInfo;
    public KV $kvAssets;
    public KV $kvAssetInfo;
    public KV $kvGroups;
    public KV $kvFunctions;
    public KV $sponsorships;
    public KV $leases;
    public Blockchain $blockchain;
    public BlockchainParser $parser;
    public BlockchainBalances $balances;
    public BlockchainData $data;

    private $kvs;
    private $recs;
    private $datarecs;
    private $workfees;
    private $workburn;
    private $workheight;
    private $qps;
    private $mts;
    private $getSponsorship;
    private $uid;
    private $contracts;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->pts = new Triples( $this->db, 'pts', 1,
            // uid                 | txkey    | type     | a        | b        | asset    | amount   | feeasset | fee      | addon    | group
            // r0                  | r1       | r2       | r3       | r4       | r5       | r6       | r7       | r8       | r9       | r10
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'TEXT',    'INTEGER', 'TEXT',    'INTEGER', 'INTEGER' ],
          //[ 0,                     1,         1,         1,         1,         1,         0,         0,         0,         0,         1 ] );
            [ 0,                     1,         0,         0,         0,         0,         0,         0,         0,         0,         0 ] );

/*
        $indexer =
        [
            'CREATE INDEX IF NOT EXISTS pts_r2_index ON pts( r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_index ON pts( r3 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_index ON pts( r4 )',
            'CREATE INDEX IF NOT EXISTS pts_r5_index ON pts( r5 )',
            'CREATE INDEX IF NOT EXISTS pts_r10_index ON pts( r10 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_r2_index ON pts( r3, r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_r2_index ON pts( r4, r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_r5_index ON pts( r3, r5 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_r5_index ON pts( r4, r5 )',
        ];
*/

        $this->balances = new BlockchainBalances( $this->db );
        $this->data = new BlockchainData( $this->db );

        $this->kvAddresses =     ( new KV( true )  )->setStorage( $this->db, 'addresses', true );
        $this->kvContracts =     ( new KV( false ) )->setStorage( $this->db, 'contracts', true, 'INTEGER PRIMARY KEY', 'INTEGER' );
        $this->kvAliasInfo =     ( new KV( false ) )->setStorage( $this->db, 'aliasInfo', true, 'INTEGER PRIMARY KEY', 'INTEGER' );
        $this->kvAssets =        ( new KV( true )  )->setStorage( $this->db, 'assets', true );
        $this->kvAssetInfo =     ( new KV( false ) )->setStorage( $this->db, 'assetInfo', true, 'INTEGER PRIMARY KEY', 'TEXT' );
        $this->kvGroups =        ( new KV( true )  )->setStorage( $this->db, 'groups', true );
        $this->kvFunctions =     ( new KV( true )  )->setStorage( $this->db, 'functions', true );

        $this->sponsorships = new KV;
        $this->leases = new KV;

        $this->kvs = [
            $this->kvAddresses,
            $this->kvContracts,
            $this->kvAliasInfo,
            $this->kvAssets,
            $this->kvAssetInfo,
            $this->kvGroups,
            $this->kvFunctions,
        ];

        $this->setHighs();
        $this->recs = [];
        $this->datarecs = [];
        $this->workheight = -1;
        $this->qps = []; // version 3 exchange price multipliers

        foreach( $this->kvContracts->db->query( 'SELECT r0 FROM contracts' ) as $value )
            $this->contracts[$value[0]] = true;
    }

    private function setSponsorship( $asset, $sponsorship )
    {
        $this->sponsorships->setKeyValue( $asset, $sponsorship );
    }

    private function getSponsorship( $asset )
    {
        $sponsorship = $this->sponsorships->getValueByKey( $asset );
        if( $sponsorship !== false )
            return $sponsorship;

        if( !isset( $this->getSponsorship ) )
        {
            $this->getSponsorship = $this->pts->db->prepare(
                'SELECT * FROM ( SELECT * FROM pts WHERE r2 =  14 AND r4 = ? ORDER BY r0 DESC LIMIT 1 ) UNION
                                 SELECT * FROM pts WHERE r2 = -14 AND r4 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->getSponsorship === false )
                w8_err( __FUNCTION__ );
        }

        if( $this->getSponsorship->execute( [ $asset, $asset ] ) === false )
            w8_err( __FUNCTION__ );

        $pts = $this->getSponsorship->fetchAll();
        if( isset( $pts[0] ) && $pts[0][AMOUNT] !== 0 )
            $sponsorship = $pts[0];
        else
            $sponsorship = 0;

        $this->setSponsorship( $asset, $sponsorship );
        return $sponsorship;
    }

    private function setLeaseInfo( $id, $tx )
    {
        $this->leases->setKeyValue( $id, $tx );
    }

    private function getLeaseInfo( $id )
    {
        $tx = $this->leases->getValueByKey( $id );
        if( $tx === false )
        {
            if( false === ( $tx = wk()->fetch( '/leasing/info/' . $id, false, null, [ 404 ] ) ) )
                w8_err( __FUNCTION__ );

            if( null === ( $tx = wk()->json_decode( $tx ) ) )
                w8_err( __FUNCTION__ );
        }

        return $tx;
    }

    public function setHighs()
    {
        $this->setUid();
    }

    private function setUid()
    {
        if( false === ( $this->uid = $this->pts->getHigh( 0 ) ) )
            $this->uid = 0;
    }

    private function getNewUid()
    {
        return ++$this->uid;
    }

    private function getSenderId( $address, bool $fake = true )
    {
        $id = $this->kvAddresses->getKeyByValue( h2b( $address ) );
        if( $id === false )
            w8_err( __FUNCTION__ );

        return $id;
    }

    private function getRecipientId( $addressOrAlias )
    {
        return $this->kvAddresses->getForcedKeyByValue( h2b( $addressOrAlias ) );
    }

    private function getFunctionId( $function )
    {
        return $this->kvFunctions->getForcedKeyByValue( $function );
    }

    private function isContract( $id )
    {
        return $this->contracts[$id] ?? false;
    }

    private function setContract( $id )
    {
        $this->contracts[$id] = true;
        $this->kvContracts->setKeyValue( $id, 1 );
    }

    private function getNewAssetId( $tx )
    {
        $id = $this->kvAssets->getForcedKeyByValue( $tx['assetId'] );
        $name = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
        $decimals = $tx['decimals'];
        $isNFT =
            true !== ( $tx['reissuable'] ?? $tx['isReissuable'] ) &&
            $tx['quantity'] === 1 &&
            $decimals === 0;
        $this->kvAssetInfo->setKeyValue( $id, ( $isNFT ? 'N' : $tx['decimals'] ) . chr( 0 ) . $name );
        return $id;
    }

    private function getUpdatedAssetId( $tx )
    {
        $id = $this->kvAssets->getKeyByValue( $tx['assetId'] );
        if( $id === false )
            w8_err( __FUNCTION__ );
        $name = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
        $info = $this->kvAssetInfo->getValueByKey( $id );
        $this->kvAssetInfo->setKeyValue( $id, substr( $info, 0, 2 ) . $name );
        return $id;
    }

    private function getAssetId( $asset )
    {
        $id = $this->kvAssets->getKeyByValue( $asset );
        if( $id === false )
            w8_err( __FUNCTION__ . ': ' . $asset  );

        return $id;
    }

    private $q_getPTS;

    private function getPTS( $from, $to )
    {
        if( !isset( $this->q_getPTS ) )
        {
            $this->q_getPTS = $this->pts->db->prepare( "SELECT * FROM pts WHERE r1 >= ? AND r1 <= ?" );
            if( $this->q_getPTS === false )
                w8_err( __FUNCTION__ );
        }

        if( $this->q_getPTS->execute( [ $from, $to ] ) === false )
            w8_err( __FUNCTION__ );

        return $this->q_getPTS->fetchAll();
    }

    private function appendTS( $ts, $fees, $burn )
    {
        $this->recs[] = $ts;
        $this->workfees = gmp_add( $this->workfees, $fees );
        $this->workburn = gmp_add( $this->workburn, $burn );
    }

    private function processGeneratorTransaction( $txkey, $info )
    {
        $block = $info['block'];
        foreach( $block['withdrawals'] as $withdrawal )
        {
            $address = $withdrawal['address'];
            $amount = gmp_mul( $withdrawal['amount'], 1000000000 );
            if( $amount > 0 )
                $this->processRewardTransaction( $txkey, [ 'type' => TX_REWARD, 'from' => REWARDER, 'to' => $address, 'amount' => $amount ] );
        }

        $miner = $this->getRecipientId( $block['miner'] );

        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_MINER,
            A =>        MINER,
            B =>        $miner,
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $this->workfees,
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    0,
        ];

        if( gmp_sign( $this->workburn ) !== 0 )
        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_BURNER,
            A =>        $miner,
            B =>        BURNER,
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $this->workburn,
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    0,
        ];
    }

    private function processRewardTransaction( $txkey, $tx )
    {
        $from = $tx['from'];
        if( $from !== REWARDER )
            $from = $this->getSenderId( $from );

        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_REWARD,
            A =>        $from,
            B =>        $this->getRecipientId( $tx['to'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    0,
        ];
    }

    private function processFailedTransaction( $txkey, $tx )
    {
        switch( $tx['type'] )
        {
            default:
                w8_err( 'processFailedTransaction unknown type: ' . $tx['type'] );
        }
    }

    private function processGenesisTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_GENESIS,
            A =>        GENESIS,
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    0,
        ], 0, 0 );
    }

    private function getQPrice( $asset )
    {
        if( $asset === 0 )
            $qp = 1;
        else
        switch( $this->kvAssetInfo->getValueByKey( $asset )[0] )
        {
            case 'N':
            case '0': $qp = 100000000; break;
            case '1': $qp = 10000000; break;
            case '2': $qp = 1000000; break;
            case '3': $qp = 100000; break;
            case '4': $qp = 10000; break;
            case '5': $qp = 1000; break;
            case '6': $qp = 100; break;
            case '7': $qp = 10; break;
            case '8': $qp = 1; break;
            default:
                w8_err();
        }
        $qps = [ 100000000 * $qp, $qp ];
        $this->qps[$asset] = $qps;
        return $qps;
    }

    private function processTransferTransaction( $txkey, $tx )
    {
        //$from = $tx['from'];
        //if( $from === '0x82300028faf9f80c6c7fbc7c832a5f41e9779e33' )
            //wk()->log( $from );
        $gasUsed = $tx['receipt']['gasUsed'];
        $fee = gmp_mul( $tx['gasPrice'], $gasUsed );
        $burn = gmp_mul( $tx['baseFee'], $gasUsed );

        if( $tx['receipt']['status'] !== '0x1' )
        if( $tx['receipt']['status'] !== '0x0' )
            w8_err( 'unexpected' );

        $failed = $tx['receipt']['status'] !== '0x1';

        if( $tx['gasPrice'] !== $tx['receipt']['effectiveGasPrice'] )
            w8_err( 'unexpected' );

        foreach( $tx['trace']['trace'] as $trace )
        {
            $action = $trace['action'];
            switch( $trace['type'] )
            {
                case 'call':
                    $to = $this->getRecipientId( $action['to'] );
                    switch( $action['callType'] )
                    {
                        case 'call':
                            $method = substr( $action['input'], 2, 8 );
                            if( $this->isContract( $to ) === false || $method === '' ) // just transfer
                            {
                                $this->appendTS( [
                                    UID =>      $this->getNewUid(),
                                    TXKEY =>    $txkey,
                                    TYPE =>     TX_TRANSFER,
                                    A =>        $this->getSenderId( $action['from'] ),
                                    B =>        $to,
                                    ASSET =>    WAVES_ASSET,
                                    AMOUNT =>   $failed ? '0' : gmp_init( $action['value'], 16 ),
                                    FEEASSET => WAVES_ASSET,
                                    FEE =>      $fee,
                                    ADDON =>    0,
                                    GROUP =>    $failed ? FAILED_GROUP : 0,
                                ], $fee, $burn );
                            }
                            else // contract call
                            {
                                $method = substr( $action['input'], 2, 8 );
                                $methodInt = intval( $method, 16 );
                                $methodStr = sprintf( '%08x', $methodInt );
                                if( $methodStr !== $method )
                                    w8_err( 'unexpected' );

                                $group = $failed ? FAILED_GROUP : $this->getGroupFunction( $to, $method, TX_INVOKE );

                                $this->appendTS( [
                                    UID =>      $this->getNewUid(),
                                    TXKEY =>    $txkey,
                                    TYPE =>     TX_INVOKE,
                                    A =>        $this->getSenderId( $action['from'] ),
                                    B =>        $to,
                                    ASSET =>    WAVES_ASSET,
                                    AMOUNT =>   $failed ? '0' : gmp_init( $action['value'], 16 ),
                                    FEEASSET => WAVES_ASSET,
                                    FEE =>      $fee,
                                    ADDON =>    0,
                                    GROUP =>    $group,
                                ], $fee, $burn );
                            }
                            break;

                        case 'delegatecall':
                            $method = substr( $action['input'], 2, 8 );
                            $contract = $this->getRecipientId( $action['to'] );
                            $methodInt = intval( $method, 16 );
                            $methodStr = sprintf( '%08x', $methodInt );
                            if( $methodStr !== $method )
                                w8_err( 'unexpected' );

                            $group = $failed ? FAILED_GROUP : $this->getGroupFunction( $contract, $method, TX_INVOKE );

                            $this->appendTS( [
                                UID =>      $this->getNewUid(),
                                TXKEY =>    $txkey,
                                TYPE =>     TX_DELEGATE,
                                A =>        $this->getSenderId( $action['from'] ),
                                B =>        $contract,
                                ASSET =>    WAVES_ASSET,
                                AMOUNT =>   $failed ? '0' : gmp_init( $action['value'], 16 ),
                                FEEASSET => WAVES_ASSET,
                                FEE =>      $fee,
                                ADDON =>    0,
                                GROUP =>    $group,
                            ], $fee, $burn );
                            break;

                        case 'staticcall':
                            $method = substr( $action['input'], 2, 8 );
                            $contract = $this->getRecipientId( $action['to'] );
                            $methodInt = intval( $method, 16 );
                            $methodStr = sprintf( '%08x', $methodInt );
                            if( $methodStr !== $method )
                                w8_err( 'unexpected' );

                            $group = $failed ? FAILED_GROUP : $this->getGroupFunction( $contract, $method, TX_INVOKE );

                            $this->appendTS( [
                                UID =>      $this->getNewUid(),
                                TXKEY =>    $txkey,
                                TYPE =>     TX_STATIC,
                                A =>        $this->getSenderId( $action['from'] ),
                                B =>        $contract,
                                ASSET =>    WAVES_ASSET,
                                AMOUNT =>   $failed ? '0' : gmp_init( $action['value'], 16 ),
                                FEEASSET => WAVES_ASSET,
                                FEE =>      $fee,
                                ADDON =>    0,
                                GROUP =>    $group,
                            ], $fee, $burn );
                            break;
                        default:
                            w8_err( 'unknown callType = ' . $action['callType'] );
                    }
                    break;

                case 'create':
                    if( $failed )
                    {
                        $B = MYSELF;
                        $AMOUNT = '0';
                        $GROUP = FAILED_GROUP;
                    }
                    else
                    {
                        $B = $this->getRecipientId( $trace['result']['address'] );
                        $AMOUNT = gmp_init( $action['value'], 16 );
                        $GROUP = 0;

                        $this->setContract( $B );
                    }
                    $this->appendTS( [
                        UID =>      $this->getNewUid(),
                        TXKEY =>    $txkey,
                        TYPE =>     TX_SMART_ACCOUNT,
                        A =>        $this->getSenderId( $action['from'] ),
                        B =>        $B,
                        ASSET =>    WAVES_ASSET,
                        AMOUNT =>   $AMOUNT,
                        FEEASSET => WAVES_ASSET,
                        FEE =>      $fee,
                        ADDON =>    0,
                        GROUP =>    $GROUP,
                    ], $fee, $burn );
                    break;

                default:
                    w8_err( 'unknown action type = ' . $trace['type'] );
            }

            if( $failed )
                break;

            $fee = 0;
            $burn = 0;
        }

        foreach( $tx['trace']['stateDiff'] as $address => $state )
        {
            $address = $this->getRecipientId( $address );
            $balance = $state['balance']['*']['to'] ?? false;
            if( $balance !== false )
                $this->traces[$address] = $balance;
        }
    }

    private function getGroupExchange( $sb, $basset, $sasset )
    {
        $groupName = $sb . $basset . ':' . $sasset;
        return $this->kvGroups->getForcedKeyByValue( $groupName );
    }

    private function getGroupFunction( $dApp, $function, $type )
    {
        return $this->kvGroups->getForcedKeyByValue( $dApp . ':' . $function . ':' . $type );
    }

    public function processTransaction( $txkey, $tx )
    {
        $height = w8k2h( $txkey );
        if( $this->workheight !== $height )
        {
            $this->flush();
            $this->workheight = $height;
            $this->workfees = gmp_init( 0 );
            $this->workburn = gmp_init( 0 );
        }

        $type = $tx['type'];
        if( $type === TX_MINER )
            return $this->processGeneratorTransaction( $txkey, $tx );
        if( $type === TX_GENESIS )
            return $this->processGenesisTransaction( $txkey, $tx );

        //$tt = microtime( true );

        switch( $type )
        {
            case '0x0':
            case '0x1':
            case '0x2':
                $this->processTransferTransaction( $txkey, $tx ); break;

            default:
                w8_err( 'unknown type = ' . $type . ' (' . $tx['hash'] . ')' );
        }

        //$this->mts[$type] += microtime( true ) - $tt;
    }

    private function flush()
    {
        if( count( $this->recs ) )
        {
            $this->pts->merge( $this->recs );
            $this->balances->update( $this->recs, false, $this->traces );
            $this->recs = [];

            if( count( $this->datarecs ) )
            {
                $this->data->update( $this->datarecs );
                $this->datarecs = [];
            }

            foreach( $this->kvs as $kv )
                $kv->merge();
        }
    }

    public function rollback( $txfrom )
    {
        // BALANCES
        $pts = $this->getPTS( $txfrom, PHP_INT_MAX );
        $this->balances->rollback( $pts );

        // DATA
        $this->data->rollback( $txfrom );

        // PTS
        $this->pts->query( 'DELETE FROM pts WHERE r1 >= '. $txfrom );
        $this->sponsorships->reset();
        $this->leases->reset();
        $this->setHighs();
        $this->workheight = -1;
    }

    private $traces;

    public function update( $txs )
    {
        $this->traces = [];
        // if global start not begin from FULL pts per block
        // append current PTS to track block fee
        foreach( $txs as $txkey => $tx )
            $this->processTransaction( $txkey, $tx );

        //$this->printMTS();
        //$this->resetMTS();

        $this->flush();
    }
}
