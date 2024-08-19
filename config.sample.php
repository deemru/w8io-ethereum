<?php

require_once __DIR__ . '/include/w8_error_handler.php';
require_once __DIR__ . '/vendor/autoload.php';
use deemru\WavesKit;

date_default_timezone_set( 'UTC' );

function wk( $full = true ) : WavesKit
{
    static $wk;

    if( !isset( $wk ) )
    {
        $wk = new WavesKit( 'E', [ 'w', 'e', 'i', 's' ] );
        if( $full )
        {
            $nodes = explode( '|', W8IO_NODES );
            define( 'WK_CURL_SETBESTONERROR', true );
            $wk->setNodeAddress( $nodes, 0 );
            $wk->setCryptash( 'SECRET_STRING_SET_YOURS_HERE' );
        }
    }

    return $wk;
}

function w8_err( $message = '(no message)' )
{
    if( isset( $_SERVER['REQUEST_URI'] ) )
        $message .= ' (' . $_SERVER['REQUEST_URI'] . ')';
    trigger_error( $message, E_USER_ERROR );
}

define( 'W8IO_ASSET', 'Unit0' );
define( 'W8IO_DECIMALS', 18 );
define( 'W8IO_NODES', 'http://127.0.0.1:8545' );
define( 'W8IO_L1_API', 'https://testnet.w8.io' );
define( 'W8IO_L1_BALANCE_DIV', 1 );
define( 'W8IO_L1_BALANCES', '3NAXNTzo37xnaTPR2cKZ5EMkTXpKVdVQ59v' );
define( 'W8IO_L1_CONTRACT', '3MsqKJ6o1ABE37676cHHBxJRs6huYTt72ch' );

define( 'W8IO_DB_DIR', __DIR__ . '/var/db/' );
define( 'W8IO_MAIN_DB', W8IO_DB_DIR . 'blockchain.sqlite3' );
define( 'W8IO_CACHE_DB', W8IO_DB_DIR . 'cache.sqlite3' );

define( 'W8IO_ROOT', '/' );
define( 'W8IO_MAX_UPDATE_BATCH', 16 );
define( 'W8IO_UPDATE_DELAY', 1 );
define( 'W8IO_OFFLINE_DELAY', 5 );
define( 'W8IO_RPC_API_TIMEOUT', 60 );
define( 'W8IO_RPC_API_CONCURENCY', 8 );
define( 'WK_CURL_TIMEOUT', W8IO_RPC_API_TIMEOUT );
define( 'W8IO_MAX_MEMORY', 1024 * 1024 * 1024 );
define( 'W8IO_ANALYTICS', '' );