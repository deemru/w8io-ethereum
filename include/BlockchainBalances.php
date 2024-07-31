<?php

namespace w8io;

use deemru\Triples;
use deemru\KV;

class BlockchainBalances
{
    public Triples $balances;
    public KV $uids;
    public KV $amounts;

    private Triples $db;
    private $uid;
    private $empty;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->balances = new Triples( $this->db, 'balances', 1,
            // uid                 | address  | asset    | balance
            // r0                  | r1       | r2       | r3
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'TEXT' ],
//          [ 0,                     1,         1,         0 ] );
            [ 0,                     0,         0,         0 ] );

/*
        $indexer =
        [
            'CREATE INDEX IF NOT EXISTS balances_r1_index ON balances( r1 )',
            'CREATE INDEX IF NOT EXISTS balances_r2_index ON balances( r2 )',
            'CREATE INDEX IF NOT EXISTS balances_r2_r3_index ON balances( r2, r3 )',
            'CREATE INDEX IF NOT EXISTS balances_r1_r2_index ON balances( r1, r2 )',
        ];
*/

        $this->uids = new KV;
        $this->amounts = new KV;
        $this->setUid();
        $this->empty = $this->uid === 0;
    }

    public function rollback( $pts )
    {
        if( is_array( $pts ) && count( $pts ) > 0 )
            $this->update( $pts, true, [] );
        $this->amounts->reset();
    }

    private function setUid()
    {
        if( false === ( $this->uid = $this->balances->getHigh( 0 ) ) )
            $this->uid = 0;
    }

    private function getNewUid()
    {
        return ++$this->uid;
    }

    public function setParser( $parser )
    {
        $this->parser = $parser;
    }

    private static function finalizeChanges( $aid, $temp_procs, &$procs )
    {
        foreach( $temp_procs as $asset => $amount )
        {
            if( $amount === 0 )
                continue;

            $procs[$aid][$asset] = gmp_add( $amount, $procs[$aid][$asset] ?? 0 );
        }
    }

    private $q_getUid;

    private function getUid( $address, $asset )
    {
        $key = $address . '_' . $asset;
        $uid = $this->uids->getValueByKey( $key );
        if( $uid !== false )
            return [ $uid, true ];

        if( !$this->empty )
        {
            if( !isset( $this->q_getUid ) )
            {
                $this->q_getUid = $this->balances->db->prepare( 'SELECT r0, r3 FROM balances WHERE r1 = ? AND r2 = ?' );
                if( $this->q_getUid === false )
                    w8_err( 'getUid' );
            }

            if( false === $this->q_getUid->execute( [ $address, $asset ] ) )
                w8_err( 'getUid' );

            $r = $this->q_getUid->fetchAll();
        }

        if( isset( $r[0] ) )
        {
            $uid = $r[0][0];
            $this->amounts->setKeyValue( $uid, $r[0][1] );
            $update = true;
        }
        else
        {
            $uid = $this->getNewUid();
            $update = false;
        }

        $this->uids->setKeyValue( $key, $uid );
        return [ $uid, $update ];
    }

    private $q_insertBalance;

    private function insertBalance( $uid, $address, $asset, $amount )
    {
        if( !isset( $this->q_insertBalance ) )
        {
            $this->q_insertBalance = $this->balances->db->prepare( 'INSERT INTO balances( r0, r1, r2, r3 ) VALUES( ?, ?, ?, ? )' );
            if( $this->q_insertBalance === false )
                w8_err( 'insertBalance' );
        }

        if( false === $this->q_insertBalance->execute( [ $uid, $address, $asset, $amount ] ) )
            w8_err( 'insertBalance' );

        $this->amounts->setKeyValue( $uid, $amount );
    }

    private $q_getBalance;
    private $q_updateBalance;

    private function updateBalance( $uid, $amount )
    {
        $before = $this->amounts->getValueByKey( $uid );
        if( $before === false )
        {
            if( !isset( $this->q_getBalance ) )
            {
                $this->q_getBalance = $this->balances->db->prepare( 'SELECT r3 FROM balances WHERE r0 = ?' );
                if( $this->q_getBalance === false )
                    w8_err( 'getBalance' );
            }

            if( false === $this->q_getBalance->execute( [ $uid ] ) )
                w8_err( 'getBalance' );

            $before = $this->q_getBalance->fetchAll()[0][0];
        }

        if( !isset( $this->q_updateBalance ) )
        {
            $this->q_updateBalance = $this->balances->db->prepare( 'UPDATE balances SET r3 = ? WHERE r0 = ?' );
            if( $this->q_updateBalance === false )
                w8_err( 'updateBalance' );
        }

        $amount = gmp_add( $before, $amount );
        if( false === $this->q_updateBalance->execute( [ $amount, $uid ] ) )
            w8_err( 'updateBalance' );

        $this->amounts->setKeyValue( $uid, $amount );
    }

    private function commitChanges( $procs, $isRollback = false )
    {
        foreach( $procs as $address => $aprocs )
        foreach( $aprocs as $asset => $amount )
        {
            if( $isRollback )
                $amount = gmp_neg( $amount );

            [ $uid, $update ] = $this->getUid( $address, $asset );

            if( $update === false )
                $this->insertBalance( $uid, $address, $asset, $amount );
            else
                $this->updateBalance( $uid, $amount );
        }
    }

    public static function processChanges( $ts, &$procs )
    {
        $type = $ts[TYPE];
        $amount = $ts[AMOUNT];
        $asset = $ts[ASSET];
        $fee = $ts[FEE];
        $afee = $ts[FEEASSET];

        switch( $type )
        {
            case TX_MINER:
            case TX_REWARD:
            case TX_TRANSFER:
            case TX_INVOKE:
            case TX_DELEGATE:
            case TX_STATIC:
            case TX_BURNER:
            case TX_SMART_ACCOUNT:
                if( $asset === $afee )
                    $procs_a = [ asset_out( $type ) => 1, $asset => gmp_neg( gmp_add( $amount, $fee ) ) ];
                else
                    $procs_a = [ asset_out( $type ) => 1, $asset => gmp_neg( $amount ), $afee => gmp_neg( $fee ) ];
                $procs_b = [ asset_in( $type ) => 1, $asset => $amount ];
                break;

            default:
                w8_err( 'unknown tx type = ' . $type );
        }

        self::finalizeChanges( $ts[A], $procs_a, $procs );
        if( isset( $procs_b ) )
            self::finalizeChanges( $ts[B], $procs_b, $procs );
    }

    public function getAllWaves()
    {
        $balances = $this->balances->db->prepare( 'SELECT r3 FROM balances WHERE r1 > 0 AND r2 = 0' );
        $balances->execute();
        $waves = 0;

        foreach( $balances as $balance )
            $waves += $balance[0];

        return $waves;
    }

    public function update( $pts, $isRollback, $traces )
    {
        $changes = [];
        foreach( $pts as $ts )
            $this->processChanges( $ts, $changes );
        $this->commitChanges( $changes, $isRollback );
        if( 10 )
        {
            foreach( $traces as $address => $balance )
            {
                [ $uid, $update ] = $this->getUid( $address, WAVES_ASSET );
                $balanceLocal = $this->amounts->getValueByKey( $uid );
                $diff = gmp_sub( $balanceLocal, $balance );
                $is2 = gmp_cmp( '2000000000000000000', $diff );
                $is4 = gmp_cmp( '4000000000000000000', $diff );
                $is6 = gmp_cmp( '6000000000000000000', $diff );
                $is8 = gmp_cmp( '8000000000000000000', $diff );
                if( gmp_sign( $diff ) !== 0 && $is2 !== 0 && $is4 !== 0 && $is6 !== 0 && $is8 !== 0 )
                {
                    require_once 'RO.php';
                    wk()->log( ( new RO( W8DB ) )->getAddressById( $address ) );
                    wk()->log( $diff );
                }
            }
        }
    }
}
