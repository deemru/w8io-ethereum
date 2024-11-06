<?php

require_once __DIR__ . '/include/w8_error_handler.php';
require_once __DIR__ . '/vendor/autoload.php';
use deemru\WavesKit;

date_default_timezone_set( 'UTC' );

// W8IO_LOCAL_RPC
function wk() : WavesKit
{
    static $wk;
    if( isset( $wk ) )
        return $wk;
    $wk = new WavesKit( 'E', [ 'w', 'e', 'i', 's' ] );
    $wk->setNodeAddress( W8IO_LOCAL_RPC, 0 );
    $wk->setCryptash( W8IO_CRYPTASH );
    $wk->curlTimeout = W8IO_RPC_API_TIMEOUT;
    return $wk;
}

// W8IO_LOCAL_ENGINE
function wke() : WavesKit
{
    static $wk;
    if( isset( $wk ) )
        return $wk;
    $wk = new WavesKit( 'E', [ 'w', 'e', 'i', 's' ] );
    $wk->setNodeAddress( W8IO_LOCAL_ENGINE, 0 );
    $wk->curlTimeout = W8IO_RPC_API_TIMEOUT;
    return $wk;
}

// W8IO_WAVES_NODE
function wkn() : WavesKit
{
    static $wk;
    if( isset( $wk ) )
        return $wk;
    $wk = new WavesKit( 'E', [ 'w', 'e', 'i', 's' ] );
    $wk->setNodeAddress( W8IO_WAVES_NODE, 0 );
    $wk->curlTimeout = W8IO_RPC_API_TIMEOUT;
    return $wk;
}

// W8IO_L1_API
function wka() : WavesKit
{
    static $wk;
    if( isset( $wk ) )
        return $wk;
    $wk = new WavesKit( 'E', [ 'w', 'e', 'i', 's' ] );
    $wk->setNodeAddress( W8IO_L1_API, 0 );
    $wk->curlTimeout = W8IO_RPC_API_TIMEOUT;
    return $wk;
}

// W8IO_OTHER_RPC
function wkr() : WavesKit
{
    static $wk;
    if( isset( $wk ) )
        return $wk;
    $wk = new WavesKit( 'E', [ 'w', 'e', 'i', 's' ] );
    $wk->setNodeAddress( W8IO_OTHER_RPC, 0 );
    $wk->curlTimeout = W8IO_RPC_API_TIMEOUT;
    return $wk;
}

function w8_err( $message = '(no message)' )
{
    if( isset( $_SERVER['REQUEST_URI'] ) )
        $message .= ' (' . $_SERVER['REQUEST_URI'] . ')';
    trigger_error( $message, E_USER_ERROR );
}

define( 'W8IO_ROOT', '/' );
define( 'W8IO_ASSET', 'UNIT0' );
define( 'W8IO_DECIMALS', 18 );
define( 'W8IO_CRYPTASH', 'SECRET_STRING_SET_YOURS_HERE' );

define( 'W8IO_LOCAL_RPC', 'http://127.0.0.1:8545' );
define( 'W8IO_LOCAL_ENGINE', 'http://127.0.0.1:8551' );
define( 'W8IO_WAVES_NODE', 'https://nodes-testnet.wavesnodes.com' );
define( 'W8IO_OTHER_RPC', 'https://unit0-testnet.w8.io' );
define( 'W8IO_L1_API', 'https://testnet.w8.io' );
define( 'W8IO_L1_ROOT', 'https://testnet.w8.io/' );
define( 'W8IO_L1_BALANCE_DIV', 1 );
define( 'W8IO_L1_CONTRACT', '3MsqKJ6o1ABE37676cHHBxJRs6huYTt72ch' );

define( 'W8IO_DB_DIR', __DIR__ . '/var/db/' );
define( 'W8IO_MAIN_DB', W8IO_DB_DIR . 'blockchain.sqlite3' );
define( 'W8IO_CACHE_DB', W8IO_DB_DIR . 'cache.sqlite3' );

define( 'W8IO_MAX_UPDATE_BATCH', 50 );
define( 'W8IO_MAX_HISTORY_BATCH', 250 );
define( 'W8IO_UPDATE_DELAY', 1 );
define( 'W8IO_OFFLINE_DELAY', 5 );
define( 'W8IO_RPC_API_TIMEOUT', 60 );
define( 'W8IO_RPC_API_CONCURENCY', 1 );
define( 'W8IO_MAX_MEMORY', 2 * 1024 * 1024 * 1024 );
define( 'W8IO_ANALYTICS', '' );