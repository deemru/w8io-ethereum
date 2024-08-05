<?php

namespace w8io;

require_once 'common.php';

use deemru\Triples;

class ROC
{
    public Triples $db;

    public function __construct()
    {
        $this->db = new Triples( W8IO_CACHE_DB );
    }

    private $getBlockByNumber;

    public function getBlockByNumber( $number )
    {
        if( !isset( $this->getBlockByNumber ) )
        {
            $this->getBlockByNumber = $this->db->db->prepare( 'SELECT r1 FROM blocks WHERE r0 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->getBlockByNumber === false )
                w8_err();
        }

        if( false === $this->getBlockByNumber->execute( [ $number ] ) )
            w8_err();

        $r = $this->getBlockByNumber->fetchAll();
        if( isset( $r[0] ) )
            return jzd( $r[0][0] );

        return false;
    }

    private $getTransactionByHash;

    public function getTransactionByHash( $hash )
    {
        if( !isset( $this->getTransactionByHash ) )
        {
            $this->getTransactionByHash = $this->db->db->prepare( 'SELECT r1 FROM txs WHERE r0 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->getTransactionByHash === false )
                w8_err();
        }

        if( false === $this->getTransactionByHash->execute( [ h2b( $hash ) ] ) )
            w8_err();

        $r = $this->getTransactionByHash->fetchAll();
        if( isset( $r[0] ) )
            return jzd( $r[0][0] );

        return false;
    }
}
