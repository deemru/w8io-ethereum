<?php

namespace w8io;
require_once 'config.php';

$z = (int)( $_COOKIE['z'] ?? 180 ); // TIMEZONE

if( isset( $_SERVER['REQUEST_URI'] ) )
    $urio = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $urio = '';

$uri = preg_filter( '/[^a-zA-Z0-9_.@\-\/]+/', '', $urio . chr( 0 ) );
if( $uri === '' )
    $uri = [ 'MINERS' ];
else
{
    if( $uri[strlen($uri) - 1] === '/' )
        $uri = substr( $uri, 0, -1 );
    $uri = explode( '/', $uri );
}

$address = $uri[0];
if( isset( $uri[1] ) )
{
    $f = $uri[1];
    if( isset( $uri[2] ) )
    {
        $arg = $uri[2];
        if( isset( $uri[3] ) )
        {
            $arg2 = $uri[3];
            if( isset( $uri[4] ) )
                $arg3 = $uri[4];
            else
                $arg3 = false;
        }
        else
        {
            $arg2 = false;
            $arg3 = false;
        }
    }
    else
    {
        $arg = false;
        $arg2 = false;
        $arg3 = false;
    }
}
else
{
    $f = false;
    $arg = false;
    $arg2 = false;
    $arg3 = false;
}

if( $address === 'api' )
{
    require_once 'include/RO.php';

    function apiexit( $code, $json )
    {
        http_response_code( $code );
        exit( json_encode( $json ) );
    }

    if( strlen( $f ) > 20 )
    {
        $wk = wk();
        if( false === ( $f = w8dec( $f ) ) ||
            false === ( $call = $wk->decryptash( $f ) ) ||
            false === ( $call = $wk->json_decode( $call ) ) )
            exit( $wk->log( 'e', 'bad API call' ) );

        switch( $call['f'] )
        {
            case 't':
            {
                $RO = new RO;

                $aid = $call['i'];
                $where = $call['w'];
                $uid = $call['u'];
                $address = $call['a'];
                $d = $call['d'] ?? 3;

                echo '<pre>';
                w8io_print_transactions( $aid, $where, $uid, 100, $address, $d );
                echo '</pre>';
                return;
            }
            case 'd':
            {
                $RO = new RO;

                $address = $call['a'];
                $aid = $call['i'];
                $begin = $call['b'];
                $limit = $call['l'];
                $string = $call['s'];

                echo '<pre>';
                [ $data, $lazy ] = w8io_get_data( $address, $aid, $begin, $limit, $string );
                $datauri = '<a href="' . W8IO_ROOT . $address . '/data/';
                $txuri = '<a href="' . W8IO_ROOT . 'tx/';
                w8io_print_data( $datauri, $txuri, $data, $lazy );
                echo '</pre>';
                return;
            }
            case 'k':
            {
                $RO = new RO;

                $address = $call['a'];
                $key = $call['k'];
                $aid = $call['i'];
                $kid = $call['d'];
                $begin = $call['b'];
                $limit = $call['l'];

                echo '<pre>';
                [ $data, $lazy ] = w8io_get_data_key( $address, $key, $aid, $kid, $begin, $limit );
                $datauri = '<a href="' . W8IO_ROOT . $address . '/data/';
                $txuri = '<a href="' . W8IO_ROOT . 'tx/';
                w8io_print_data( $datauri, $txuri, $data, $lazy, true, $RO );
                echo '</pre>';
                return;
            }
        }

        exit( $wk->log( 'e', 'bad API call' ) );
    }
    else
    if( $f === 'height' )
    {
        require_once 'include/RO.php';
        $RO = new RO;
        $json = $RO->getLastHeightTimestamp();
        if( $json === false )
            apiexit( 503, [ 'code' => 503, 'message' => 'database unavailable' ] );
        apiexit( 200, $json[0] );
    }
    else
    if( $f === 'alive' )
    {
        require_once 'include/RO.php';
        $RO = new RO;
        $json = $RO->getLastHeightTimestamp();
        if( $json === false )
            apiexit( 503, [ 'code' => 503, 'message' => 'database unavailable' ] );
        $now = time();
        $dbts = $json[1];
        $diff = $now - $dbts;
        $threshold = $arg === false ? 600 : intval( $arg );
        if( $diff > $threshold )
            apiexit( 503, [ 'code' => 503, 'message' => "too big diff: $now - $dbts = $diff > $threshold" ] );
        apiexit( 200, $diff );
    }

    exit( http_response_code( 404 ) );
}

if( $address === 'j13' )
{
    require_once 'include/RO.php';
    $RO = new RO;
    $q = $RO->db->query( 'SELECT * FROM pts WHERE r2 = 13 ORDER BY r0 DESC LIMIT 100' );
    $n = 0;
    $json = [];
    $max = 100;
    if( $f !== false )
        $max = min( (int)$f, $max );
    foreach( $q as $r )
    {
        $txkey = $r[1];
        $txid = $RO->getTxIdByTxKey( $txkey );
        $json[] = $txid;
        if( ++$n >= $max )
            break;
    }

    exit( json_encode( $json ) );
}

if( $address === 'tx' && is_numeric( $f ) )
{
    require_once 'include/RO.php';
    $RO = new RO;

    if( w8k2h( $f ) === w8k2h( $f + 1 ) - 1 )
        exit( header( 'location: ' . W8IO_ROOT . 'b/' . w8k2h( $f ) ) );

    $txid = $RO->getTxIdByTxKey( $f );
    if( $txid !== false )
        exit( header( 'location: ' . W8IO_ROOT . 'tx/' . $txid ) );
}
else
if( $address === 'tk' && $f !== false && strlen( $f ) >= 32 )
{
    require_once 'include/RO.php';
    $RO = new RO;

    $txkey = $RO->getTxKeyByTxId( $f );
    exit( (string)$txkey );
}
else
if( $address === 'top' && $f !== false )
{
    require_once 'include/RO.php';
    $RO = new RO;

    if( is_numeric( $f ) )
    {
        $f = $RO->getAssetById( $f );
        if( $f !== false )
            exit( header( 'location: ' . W8IO_ROOT . 'top/' . $f ) );
    }

    $aid = $f === W8IO_ASSET ? 0 : $RO->getIdByAsset( $f );
    if( $aid !== false )
    {
        $info = $RO->getAssetInfoById( $aid );
        if( $info !== false )
        {
            if( $arg === false )
                $arg = 1000;
            else if( $arg > 10000 )
                exit( header( 'location: ' . W8IO_ROOT . 'top/' . $f . '/10000' ) );
        }
        else
        {
            unset( $info );
            unset( $aid );
        }
    }
}
else
if( $address === 'MINERS' )
{
    //$showtime = true;

    if( $f === false )
        $f = 1472 * 4;

    $f = intval( $f );
    $n = min( max( $f, isset( $showtime ) ? 1 : 80 ), 100000 );
    if( $n !== $f )
        exit( header("location: " . W8IO_ROOT . "$address/$n" ) );

    if( isset( $showtime ) )
    {
        $showfile = "MINERS-$arg-$f.html";
        if( file_exists( $showfile ) )
            exit( file_get_contents( $showfile ) );

        ob_start();
    }
}

function prettyAddress( $address )
{
    if( strlen( $address ) === 42 )
        return substr( $address, 0, 6 ) . '&#183;&#183;&#183;' . substr( $address, -4 );
    return $address;
}

if( strlen( $address ) > 42 )
{
    $f = $address;
    $address = 'tx';
}

function prolog()
{
    global $address;
    global $f;
    global $L;

    $L = (int)( $_COOKIE['L'] ?? 0 ) === 1;
    $title = 'w8 &#183; ' . prettyAddress( $address );
    if( $f !== false && ( is_numeric( $f ) || strlen( $f ) >= 32 || $f === W8IO_ASSET ) )
        $title .= ' &#183; ' . prettyAddress( $f );
    echo sprintf( '
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="format-detection" content="telephone=no">
        <meta name="format-detection" content="date=no">
        <meta name="format-detection" content="address=no">
        <meta name="format-detection" content="email=no">
        <title>%s</title>
        <link rel="shortcut icon" href="/static/favicon8.ico" type="image/x-icon">
        <link rel="stylesheet" href="/static/fonts.css">
        <link rel="stylesheet" href="/static/static%s.css">
        <script type="text/javascript" src="/static/jquery.js" charset="UTF-8"></script>
        <script type="text/javascript" src="/static/static.js" charset="UTF-8"></script>
    </head>
    <body>
        <pre>
', $title, $L ? '-l' : '-n' );
}

function w8io_get_data( $address, $aid, $begin, $limit, $string )
{
    global $RO;

    $rs = $RO->getKVsByAddress( $aid, $begin, $limit + 1 );
    $n = 0;
    foreach( $rs as [ $r0, $r1, /*$r2*/, /*$r3*/, $r4, $r5, $r6 ] )
    {
        if( ++$n > $limit )
        {
            $wk = wk();
            $call = [
                'f' => 'd',
                'a' => $address,
                'i' => $aid,
                'b' => $r0,
                'l' => $limit,
                's' => $string,
            ];
            $call = W8IO_ROOT . 'api/' . w8enc( $wk->encryptash( json_encode( $call ) ) );
            $lazy = '</pre><pre class="lazyload" url="' . $call . '">...';
            break;
        }

        if( $r6 === TYPE_NULL )
            continue;

        $key = $RO->getKeyById( $r4 );
        if( $string === '' || false !== strpos( $key, $string ) )
        {
            $value = $RO->getValueByTypeId( $r5, $r6 );
            $data[] = [ 'key' => $key, 'type' => DATA_TYPE_STRINGS[$r6], 'value' => $value, 'txkey' => $r1 ];
        }
    }

    return [ $data ?? [], $lazy ?? false ];
}

function w8io_get_data_key( $address, $key, $aid, $kid, $begin, $limit )
{
    global $RO;

    $rs = $RO->getKVsByAddressKey( $aid, $kid, $begin, $limit + 1 );
    $n = 0;
    foreach( $rs as [ $r0, $r1, /*$r2*/, /*$r3*/, /*$r4*/, $r5, $r6 ] )
    {
        if( ++$n > $limit )
        {
            $wk = wk();
            $call = [
                'f' => 'k',
                'a' => $address,
                'k' => $key,
                'i' => $aid,
                'd' => $kid,
                'b' => $r0,
                'l' => $limit,
            ];
            $call = W8IO_ROOT . 'api/' . w8enc( $wk->encryptash( json_encode( $call ) ) );
            $lazy = '</pre><pre class="lazyload" url="' . $call . '">...';
            break;
        }

        $value = $r6 === TYPE_NULL ? 'null' : $RO->getValueByTypeId( $r5, $r6 );
        $data[] = [ 'key' => $key, 'type' => DATA_TYPE_STRINGS[$r6], 'value' => $value, 'txkey' => $r1 ];
    }

    return [ $data ?? [], $lazy ?? false ];
}

function w8io_print_data( $datauri, $txuri, $data, $lazy, $changelog = false, $RO = null )
{
    $n = 0;
    $c = count( $data );
    $lastblock = 0;
    global $z;
    foreach( $data as $r )
    {
        $k = htmlentities( $r['key'] );
        $t = $r['type'];
        if( $t === 'string' )
            $v = '"' . htmlentities( $r['value'] ) . '"';
        else if( $t === 'binary' )
            $v = '"' . $r['value'] . '"';
        else if( $t === 'boolean' )
            $v = $r['value'] ? 'true' : 'false';
        else
            $v = $r['value'];
        $comma = ( $lazy === false && ++$n >= $c ) ? '' : ',';

        if( $changelog )
        {
            $txkey = $r['txkey'];
            $block = w8k2h( $txkey );
            if( $lastblock !== $block )
            {
                $lastblock = $block;
                $date = date( 'Y.m.d H:i', $RO->getTimestampByHeight( $block ) + $z * 60 );
            }
            echo PHP_EOL . '    "' . $txuri . $txkey .'">' . $date . '</a>": ' . $v . $comma;
        }
        else
            echo PHP_EOL . '    "' . $datauri . urlencode( $k ) . '">' . $k . '</a>": ' . $txuri . $r['txkey'] .'">' . $v . '</a>' . $comma;
    }

    if( $lazy === false )
        echo PHP_EOL . '}';
    else
        echo PHP_EOL . $lazy;
}

function w8io_get_txkey_data( $txkey )
{
    global $RO;

    $rs = $RO->getKVsByTxKey( $txkey );
    foreach( $rs as $r )
    {
        $prev = $RO->getPreviousKV( $r[3], $r[4], $r[0] );
        $data[] = [ $r, $prev ];
    }

    return $data ?? [];
}

function w8io_print_distribution( $f, $aid, $info, $n )
{
    global $z;
    global $RO;

    $decimals = ord( $info[0] );
    $asset = substr( $info, 2 );

    $balances = $RO->db->query( 'SELECT * FROM balances WHERE r2 = ' . $aid . ' ORDER BY r4 DESC LIMIT ' . $n );
    $total = 0;
    $n = 0;
    $out = '';
    foreach( $balances as $balance )
    {
        $amount = gmp_init( $balance[3] );
        if( gmp_sign( $amount) <= 0 )
            break;
        $total = gmp_add( $total, $amount );
        $aid = $balance[1];
        $address = $RO->getAddressById( $aid );
        $out .= str_pad( ++$n, 5, ' ', STR_PAD_LEFT ) . ') <a href="' . W8IO_ROOT . $address . '/f/' . $f . '">' . $address . '</a>: ' . w8io_amount( $amount, $decimals ) . PHP_EOL;
    }

    $heightTime = $RO->getLastHeightTimestamp();
    $time = date( 'Y.m.d H:i', $heightTime[1] + $z * 60 );
    $height = $heightTime[0];

    echo 'Top ' . $n . ' (' . $asset .') @ ' . $height . ' <small>(' . $time . ')</small>'. PHP_EOL . PHP_EOL;
    echo str_pad( 'Top ' . $n . ' balance: ', 44, ' ', STR_PAD_LEFT ) . w8io_amount( $total, $decimals ) . PHP_EOL . PHP_EOL;
    echo $out;
}

function w8io_sign( $sign )
{
    if( $sign > 0 )
        return '+';
    if( $sign < 0 )
        return '-';
    return '';
}

function w8io_print_transactions( $aid, $where, $uid, $count, $address, $d, $summary = false )
{
    global $RO;
    global $z;

    $pts = $RO->getPTSByAddressId( $aid, $where, $count + 1, $uid, $d );

    if( $summary )
    {
        require_once 'include/BlockchainBalances.php';
        $changes = [];
        foreach( $pts as $ts )
            BlockchainBalances::processChanges( $ts, $changes );
        $diff = '';
        {
            $last_a = '';
            foreach( $changes as $a => $diffs )
            {
                if( $a <= 0 )
                    continue;

                $a = $RO->getAddressById( $a );
                foreach( $diffs as $asset => $amount )
                {
                    if( gmp_sign( $amount ) === 0 )
                        continue;
                    
                    if( $asset > 0 )
                    {
                        $sign = $amount < 0 ? -1 : 1;
                        $info = $RO->getAssetInfoById( $asset );
                        $name = substr( $info, 2 );
                        $decimals = ord( $info[0] );
                        $amount = w8io_amount( $amount, $decimals, 0, false );
                        $amount = ' ' . w8io_sign( $sign ) . $amount;
                        $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . $asset . '">' . $name . '</a>';
                    }
                    else if( $asset === 0 )
                    {
                        $sign = $amount < 0 ? -1 : 1;
                        $amount = ' ' . w8io_sign( $sign ) . w8io_amount( $amount, 18, 0, false );
                        $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . W8IO_ASSET . '">' . W8IO_ASSET . '</a>';
                    }
                    else if( $asset === WAVES_LEASE_ASSET )
                    {
                        $sign = $amount < 0 ? -1 : 1;
                        $amount = ' ' . w8io_sign( $sign ) . w8io_amount( $amount, 18, 0, false );
                        $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . W8IO_ASSET . '">' . W8IO_ASSET . ' (MINER)</a>';
                    }
                    else
                        continue;
                    
                    if( $last_a === $a )
                    {
                        $diff .= '<span>———————————————————————————————————</span>: ' . $amount . $asset . PHP_EOL;
                    }
                    else
                    {
                        $last_a = $a;
                        if( strlen( $a ) === 42 )
                            $diff .= w8io_a( $a ) . ': ' . $amount . $asset . PHP_EOL;
                        else
                            $diff .= '<span>' . str_repeat( '—', 41 - strlen( $a ) ) . ' </span>' . w8io_a( $a ) . ': ' . $amount . $asset . PHP_EOL;
                    }
                }
            }

            if( $diff !== '' )
            {
                echo $diff;
                echo PHP_EOL;
            }
        }
    }

    $maxlen1 = 0;
    $maxlen2 = 0;
    $outs = [];
    $tdb = [];
    $lastblock = -1;

    $n = 0;
    foreach( $pts as $ts )
    {
        if( $count && ++$n > $count )
        {
            $wk = wk();
            $call = [
                'f' => 't',
                'i' => $aid,
                'w' => $where,
                'u' => $ts[UID],
                'a' => $address,
                'd' => $d,
            ];
            $call = W8IO_ROOT . 'api/' . w8enc( $wk->encryptash( json_encode( $call ) ) );
            $lazy = '</pre><pre class="lazyload" url="' . $call . '">...';
            break;
        }

        $type = $ts[TYPE];
        $asset = $ts[ASSET];
        $amount = $ts[AMOUNT];
        $a = $ts[A];
        $b = $ts[B];
        $aspam = false;
        unset( $reclen );
        if( $aid !== false && $b === MYSELF )
        {
            $b = $a;
            $isa = true;
            $isb = true;

            if( $asset === NO_ASSET )
            {
                $amount = '';
                $asset = '';
                $reclen = -1;
            }
        }
        else
        {
            $isa = $a === $aid;
            $isb = $b === $aid;
        }

        if( !isset( $reclen ) )
        {
            if( $asset > 0 )
            {
                if( $type === TX_ISSUE || $type === TX_REISSUE )
                    $sign = 1;
                else if( $type === TX_BURN )
                    $sign = -1;
                else
                    $sign = ( $amount < 0 ? -1 : 1 ) * ( $isb ? ( $isa ? 0 : 1 ) : -1 );

                $info = $RO->getAssetInfoById( $asset );
                if( $info[1] === chr( 1 ) )
                    $aspam = true;

                $name = substr( $info, 2 );
                $decimals = ord( $info[0] );
                $amount = w8io_amount( $amount, $decimals, 0, false );
                $amount = ' ' . w8io_sign( $sign ) . $amount;
                $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . $asset . '">' . $name . '</a>';
                $reclen = strlen( $amount ) + mb_strlen( html_entity_decode( $name ), 'UTF-8' );
            }
            else if( $amount === '0' )
            {
                $amount = '';
                $asset = '';
                $reclen = -1;
            }
            else
            {
                $sign = ( ( $amount < 0 ) ? -1 : 1 ) * ( $isb ? ( $isa ? 0 : 1 ) : -1 );
                $amount = ' ' . w8io_sign( $sign ) . w8io_amount( $amount, 18, 0, false );
                $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . W8IO_ASSET . '">' . W8IO_ASSET . '</a>';
                $reclen = strlen( $amount ) + 5;
            }
        }

        $a = $isa ? $address : $RO->getAddressById( $a );
        $b = $isb ? $address : $RO->getAddressById( $b );

        $fee = $ts[FEE];

        if( $isa && $fee !== '0' )
        {
            $afee = $ts[FEEASSET];

            if( $afee )
            {
                $info = $RO->getAssetInfoById( $afee );
                $decimals = ord( $info[0] );
                $fee = ' <small>' . w8io_amount( $fee, $decimals, 0 ) . ' <a href="' . W8IO_ROOT . $address . '/f/' . $afee . '">' . substr( $info, 2 ) . '</a></small>';
            }
            else
            {
                $fee = ' <small>' . w8io_amount( $fee, 18, 0 ) . ' <a href="' . W8IO_ROOT . $address . '/f/' . W8IO_ASSET . '">' . W8IO_ASSET . '</a></small>';
            }
        }
        else
            $fee = '';

        $addon = $ts[ADDON];
        $linklen = 0;

        if( $addon === 0 )
            $addon = '';
        else
        {
            if( isAliasType( $type ) )
            {
                $b = $RO->getAliasById( $addon );
                $addon = '';
                $isb = false;
            }
            else if( $type === TX_EXCHANGE )
            {
                $groupId = $ts[GROUP];

                if( isset( $tdb[$groupId] ) )
                {
                    [ $bdecimals, $bname, $sdecimals, $sname, $link, $linklen ] = $tdb[$groupId];
                }
                else
                {
                    $group = $RO->getGroupById( $groupId );
                    if( $group === false )
                        w8_err( "getGroupById( $groupId )" );
                    $pair = explode( ':', substr( $group, 1 ) );
                    $buy = $RO->getAssetInfoById( (int)$pair[0] );
                    $sell = $RO->getAssetInfoById( (int)$pair[1] );

                    $bdecimals = ord( $buy[0] );
                    $bname = substr( $buy, 2 );
                    $sdecimals = ord( $sell[0] );
                    $sname = substr( $sell, 2 );

                    $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                    $linklen = strlen( $link ) + 3;

                    $tdb[$groupId] = [ $bdecimals, $bname, $sdecimals, $sname, $link, $linklen ];
                }

                $price = w8io_amount( $addon, $sdecimals, 0 );

                $addon = ' ' . $price . $link . $bname . '/' . $sname . '</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );
            }
            else
                $addon = '';
        }

        //if( $type === TX_INVOKE || $type === TX_DELEGATE )
        {
            $groupId = $ts[GROUP];

            if( isset( $tdb[$groupId] ) )
            {
                [ $link, $linklen, $addon, $maxlen ] = $tdb[$groupId];
                if( $maxlen > $maxlen2 )
                    $maxlen2 = $maxlen;
            }
            if( $groupId > 0 )
            {
                $group = $RO->getGroupById( $groupId );
                if( $group === false )
                    w8_err( "getGroupById( $groupId )" );
                $method = explode( ':', $group )[1];
                $addon = $RO->getFunctionById( $method );
                if( $addon === false )
                    $addon = $method;

                $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                $linklen = strlen( $link ) + 3;
                $addon = $link . htmlentities( $addon ) . '()</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );

                $tdb[$groupId] = [ $link, $linklen, $addon, $maxlen2 ];
            }
            else if( $groupId !== 0 )
            {
                if( $groupId === FAILED_GROUP )
                    $addon = '(failed)';
                else if( $groupId === ETHEREUM_TRANSFER_GROUP )
                    $addon = '(transfer)';
                else
                    w8_err( "unknown $groupId" );

                $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                $linklen = strlen( $link ) + 3;
                $addon = $link . $addon . '</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );

                $tdb[$groupId] = [ $link, $linklen, $addon, $maxlen2 ];
            }
        }

        $wtype = TYPE_STRINGS[$type];
        $reclen += strlen( $wtype );
        $maxlen1 = max( $maxlen1, $reclen );
        $block = w8k2h( $ts[TXKEY] );

        if( $lastblock !== $block )
        {
            $lastblock = $block;
            $date = date( 'Y.m.d H:i', $RO->getTimestampByHeight( $block ) + $z * 60 );
        }

        if( $aid )
        {
            $otx = $isa && $type > 0;
            $bld = $otx || ( $isb && ( $type === TX_INVOKE || $type === ITX_INVOKE ) );
            $act = $otx ? '<small>&#183; ' : '<small>&nbsp; ';
            $tar = $isa ? ( $isb ? '<>' : w8io_a( $b ) ) : w8io_a( $a );
            $blk = $type > 0 || ( $isb && $type === ITX_INVOKE ) ? '&nbsp;&#183; </small><a href="' : '&nbsp;&nbsp; </small><a href="';

            {
                $txkey = '<a href="' . W8IO_ROOT . 'tx/' . $ts[TXKEY] . '">' . $date . '</a>';

                if( $aspam )
                    $fee .= ' <small>spam</small>';

                $outs[] = [
                    $act,
                    ( $bld ? '<b>' : '' ) . $act . $txkey . $blk .
                    W8IO_ROOT . $address . '/t/' . $type . '">' . $wtype . '</a>' . $amount . $asset . ( $bld ? '</b>' : '' ),
                    $reclen,
                    $addon, $linklen,
                    $tar . $fee,
                ];
            }
        }
        else
        {
            if( isset( $amount[1] ) && $amount[1] === '-' )
                $amount = ' ' . substr( $amount, 2 );

            $fee = '';
            if( $aspam )
                $fee .= ' <small>spam</small>';

            echo
                '<small><a href="' . W8IO_ROOT . 'tx/' . $ts[TXKEY] . '">' . $date . '</a>' .
                ' <a href="' . W8IO_ROOT . 'b/' . $block . '">[' . $block . ']</a></small>' .
                ' <a href="' . W8IO_ROOT . $address . '/t/' . $type . '">' . $wtype . '</a> ' . w8io_a( $a ) . ' > ' . w8io_a( $b ) .
                $addon . $amount . $asset . $fee . PHP_EOL;
        }
    }

    foreach( $outs as $out )
    {
        $act = $out[0];
        $p1 = $out[1];
        $p1pad = $out[2];
        $addon = $out[3];
        $p2pad = mb_strlen( html_entity_decode( $addon ), 'UTF-8' );
        $p3 = $out[5];

        if( $p2pad === 0 )
        {
            $p1pad = $maxlen1 - $p1pad + $maxlen2;
            $pad2 = '';
        }
        else
        {
            $p1pad = $maxlen1 - $p1pad;
            $p2pad = $maxlen2 - ( $p2pad - $out[4] );
            $pad2 = $p2pad > 3 ? ( ' <span>' . str_repeat( '—', $p2pad - 1 ) . '</span>' ) : str_repeat( ' ', $p2pad );
        }
        $pad1 = $p1pad > 3 ? ( ' <span>' . str_repeat( '—', $p1pad - 1 ) . '</span>' ) : str_repeat( ' ', $p1pad );

        echo $p1 . $pad1 . $addon . $pad2 . ' ' . $p3 . PHP_EOL;
    }

    if( isset( $lazy ) )
        echo $lazy;
}

function w8io_a( $address, $asset = null )
{
    if( isset( $address[5] ) && $address[5] === ':' )
        $address = substr( $address, 8 );
    $f = isset( $asset ) ? ( '/f/' . $asset ) : '';
    return '<a href=' . W8IO_ROOT . $address . $f . '>' . $address . '</a>';
}

function is_address( $value )
{
    return is_string( $value ) && strlen( $value ) === 42;
}

function is_hash( $value )
{
    return is_string( $value ) && strlen( $value ) === 66;
}

function w8io_height( $height )
{
    return '<a href=' . W8IO_ROOT . 'b/' . $height . '>' . $height . '</a>';
}

function w8io_txid( $txid )
{
    return '<a href=' . W8IO_ROOT . 'tx/' . $txid . '>' . $txid . '</a>';
}

function htmlfilter( $kv )
{
    $fkv = [];
    foreach( $kv as $k => $v )
        if( is_array( $v ) )
        {
            $fkv[$k] = htmlfilter( $v );
        }
        else
        {
            if( is_string( $k ) )
                switch( $k )
                {
                    case 'transactionHash':
                    case 'hash':
                        if( is_hash( $v ) )
                            $v = w8io_txid( $v );
                        break;
                    case 'from':
                    case 'to':
                        if( is_address( $v ) )
                            $v = w8io_a( $v );
                        break;
                    default:
                        if( !isset( $v ) )
                            $v = null;
                        else
                        if( is_string( $v ) )
                        {
                            $v = htmlentities( $v );
                            if( is_address( $v ) )
                                $v = w8io_a( $v );
                        }
                }

            $fkv[$k] = $v;
        }

    return $fkv;
}

function htmlscript( $tx, $txkey, $txid, $compacted )
{
    $headers = $compacted ? [ 'compacted: true' ] : null;

    if( empty( $tx['script'] ) )
        $decompile1 = '# no script';
    else
    {
        $decompile = wk()->fetch( '/utils/script/decompile', true, $tx['script'], null, $headers );
        if( $decompile === false )
            return;
        $decompile = wk()->json_decode( $decompile );
        if( $decompile === false )
            return;
        $decompile1 = $decompile['script'];
    }

    require_once 'include/RO.php';
    $RO = new RO;
    $a = $RO->getAddressIdByAddress( $tx['sender'] );
    if( $a === false )
        return;

    if( $tx['type'] === 13 )
    {
        $prevScript = $RO->db->query( 'SELECT * FROM pts WHERE r3 = ' . $a . ' AND r2 = 13 AND r1 < ' . $txkey . ' ORDER BY r0 DESC LIMIT 1' );
        $nextScript = $RO->db->query( 'SELECT * FROM pts WHERE r3 = ' . $a . ' AND r2 = 13 AND r1 > ' . $txkey . ' ORDER BY r0 ASC LIMIT 1' );
    }
    else
    {
        $assetId = $RO->getIdByAsset( $tx['assetId'] );
        $prevScript = $RO->db->query( 'SELECT * FROM pts WHERE r3 = ' . $a . ' AND r2 = 15 AND r5 = ' . $assetId . ' AND r1 < ' . $txkey . ' ORDER BY r0 DESC LIMIT 1' );
        $nextScript = $RO->db->query( 'SELECT * FROM pts WHERE r3 = ' . $a . ' AND r2 = 15 AND r5 = ' . $assetId . ' AND r1 > ' . $txkey . ' ORDER BY r0 ASC LIMIT 1' );
    }

    $viewMode = 'View: ' . ( $compacted ? ( '<a href="' . W8IO_ROOT . 'tx/' . $txid . '">original</a> | compacted' ) : ( 'original | <a href="' . W8IO_ROOT . 'tx/' . $txid . '/compacted">compacted</a>' ) );

    $r = $prevScript->fetchAll();
    if( !isset( $r[0][1] ) )
    {
        $decompile2 = '# no script';
        $result = $viewMode . PHP_EOL . 'Prev: none' . PHP_EOL;
    }
    else
    {
        $txidPrev = $RO->getTxIdByTxKey( $r[0][1] );
        $result = $viewMode . PHP_EOL . 'Prev: ' . ( $tx === false ? 'ERROR' : w8io_txid( $txidPrev, null ) ) . PHP_EOL;

        $txPrev = wk()->getTransactionById( $txidPrev );
        if( empty( $txPrev['script'] ) )
            $decompile2 = '# no script';
        else
        {
            $decompile = wk()->fetch( '/utils/script/decompile', true, $txPrev['script'], null, $headers );
            if( $decompile === false )
                return;
            $decompile = wk()->json_decode( $decompile );
            if( $decompile === false )
                return;
            $decompile2 = $decompile['script'];
        }
    }

    $r = $nextScript->fetchAll();
    if( !isset( $r[0][1] ) )
    {
        $result .= 'Next: none' . PHP_EOL . PHP_EOL;
    }
    else
    {
        $txidNext = $RO->getTxIdByTxKey( $r[0][1] );
        $result .= 'Next: ' . w8io_txid( $txidNext, null ) . PHP_EOL . PHP_EOL;
    }

    $result .= '<style>' . \Jfcherng\Diff\DiffHelper::getStyleSheet() . '</style>';
    $full = \Jfcherng\Diff\DiffHelper::calculate( $decompile2, $decompile1, 'Inline', [ 'context' => \Jfcherng\Diff\Differ::CONTEXT_ALL, 'fullContextIfIdentical' => true ], [ 'detailLevel' => 'word' ] );
    if( $decompile1 !== $decompile2 )
    {
        $diff = \Jfcherng\Diff\DiffHelper::calculate( $decompile2, $decompile1, 'Inline', [], [ 'detailLevel' => 'word' ] );
        if( $diff !== $full )
            $result .= 'Diff: ' . PHP_EOL . $diff . PHP_EOL;
    }
    $result .= 'Full: ' . PHP_EOL . $full;

    return $result;
}

function w8io_tv_string( $t, $v )
{
    if( $t === TYPE_STRING )
        return '"' . htmlentities( $v ) . '"';
    else if( $t === TYPE_INTEGER )
        return $v;
    else if( $t === TYPE_BINARY )
        return '"' . $v . '"';
    else if( $t === TYPE_BOOLEAN )
        return $v ? 'true' : 'false';
    else if( $t === TYPE_NULL )
        return 'null';
    else
        w8_err( "w8io_tv_string( $t, $v )" );
}

if( $address === 'tx' && $f !== false )
{
    if( strlen( $f ) === 66 )
    {
        prolog();
        require_once 'include/RO.php';
        $RO = new RO;
        $txid = $RO->getTxKeyByTxId( $f );
        if( $txid === false )
        {
            $txid = $RO->getIdByAsset( $f );
            if( $txid !== false )
            {
                $txid = $RO->getFirstTxByAssetId( $txid );
                $f = $RO->getTxIdByTxKey( $txid );
            }
        }

        if( $txid === false )
            echo json_encode( [ 'error' => "getTxKeyByTxId( $f ) failed" ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
        else
        {
            require_once 'include/ROC.php';
            $ROC = new ROC;
            $tx = $ROC->getTransactionByHash( $f );
            if( $tx === false )
                echo json_encode( [ 'error' => "getTransactionById( $f ) failed" ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
            else
            {
                echo 'tx &#183; <a href="' . W8IO_ROOT . 'tx/' . $f . '">' . $f . '</a>' . PHP_EOL . PHP_EOL;
                w8io_print_transactions( false, 'r1 = ' . $txid, false, 1000, 'txs', 3, true );

                $data = w8io_get_txkey_data( $txid );
                if( count( $data ) > 0 )
                {
                    $lastaid = null;
                    $txkeyuri = '</a>: <a href="' . W8IO_ROOT . 'tx/';
                    foreach( $data as [ $r, $pr ] )
                    {
                        $aid = $r[3];
                        if( $lastaid !== $aid )
                        {
                            $address = $RO->getAddressById( $r[3] );
                            $datauri = '<a href="' . W8IO_ROOT . $address . '">' . $address . '</a>: <a href="' . W8IO_ROOT . $address . '/data/';
                            $lastaid = $aid;
                        }

                        $k = $RO->getKeyById( $r[4] );
                        $v = $RO->getValueByTypeId( $r[5], $r[6] );
                        $t = $r[6];
                        if( $pr === false )
                        {
                            $prev = '</a>: ';
                        }
                        else
                        if( $pr[5] === $r[5] && $pr[6] === $r[6] )
                        {
                            $pv = $v;
                            $pt = $t;
                            $prev = $txkeyuri . $pr[1] . '">' . w8io_tv_string( $pt, $pv ) . '</a> == ';
                        }
                        else
                        {
                            $pv = $RO->getValueByTypeId( $pr[5], $pr[6] );
                            $pt = $pr[6];
                            $prev = $txkeyuri . $pr[1] . '">' . w8io_tv_string( $pt, $pv ) . '</a> -> ';
                        }
                        {
                            $k = htmlentities( $k );
                            echo PHP_EOL . $datauri . urlencode( $k ) . '">' . $k . $prev . w8io_tv_string( $t, $v );
                        }
                    }

                    echo PHP_EOL;
                }

                if( in_array( $tx['type'], [ 13, 15 ] ) )
                    $addon = htmlscript( $tx, $txid, $f, $arg === 'compacted' );
                $tx = htmlfilter( $tx );
                echo '<br>' . json_encode( $tx, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
                if( isset( $addon ) )
                    echo PHP_EOL . PHP_EOL . $addon;
            }
        }
    }
}
else
if( $address === 'b' )
{
    prolog();
    $height = (int)$f;
    require_once 'include/ROC.php';
    $ROC = new ROC;
    $block = $ROC->getBlockByNumber( $height );

    if( $block === false )
    {
        echo 'block not found';
    }
    else
    {
        $firsts = [];
        
        if( $height > 0 )
            $firsts['previous'] = w8io_height( $height - 1 );
        $firsts['height'] = w8io_height( $height );
        $firsts['next'] = w8io_height( $height + 1 );

        $txs = $block['transactions'];
        unset( $block['transactions'] );
        $block['miner'] = w8io_a( $block['miner'] );
        if( $height > 1 )
        {
            unset( $block['height'] );
            $block['previous'] = w8io_height( $height - 1 );
        }
        $block['height'] = w8io_height( $height );
        $block['next'] = w8io_height( $height + 1 );
        $ftxs = [];
        foreach( $txs as $v )
            $ftxs[] = w8io_txid( $v );
        $block['transactionsCount'] = count( $ftxs );
        $block['transactions'] = $ftxs;
        $block = array_merge( $firsts, $block );
        echo json_encode( $block, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
    }
}
else
if( $address === 'top' && isset( $info ) )
{
    prolog();
    echo '<pre>';
    w8io_print_distribution( $f, $aid, $info, (int)$arg );
    echo '</pre>';
}
else
if( $address === 'MINERS' )
{
    prolog();
    $arg = $arg !== false ? intval( $arg ) : null;

    require_once 'include/RO.php';
    $RO = new RO;

    $map_addresses_file = W8IO_DB_DIR . 'map_addresses.json';
    $map_balances_file = W8IO_DB_DIR . 'map_balances.json';
    if( file_exists( $map_addresses_file ) && time() - filemtime( $map_addresses_file ) < 300 )
    {
        $map_addresses = jd( file_get_contents( $map_addresses_file ) );
        $map_balances = jd( file_get_contents( $map_balances_file ) );
    }
    else
    {
        $L1_API = \deemru\Fetcher::host( W8IO_L1_API );
        $L1_BALANCES = $L1_API->fetch( '/api/data/' . W8IO_L1_BALANCES );
        $L1_BALANCES = $L1_BALANCES === false ? [] : jd( $L1_BALANCES );
        $L1_MINERS = $L1_API->fetch( '/api/data/' . W8IO_L1_CONTRACT . '/allMiners' );
        $L1_MINERS = $L1_MINERS === false ? false : jd( $L1_MINERS );
        $L1_MINERS = $L1_MINERS['allMiners'] ?? '';
        $L1_MINERS = explode( ',', $L1_MINERS );
        
        $map_balances = [];
        foreach( $L1_BALANCES as $k => $v )
        {
            $address = substr( $k, 4 );
            $balance = explode( '__', $v );
            $balance = (int)( $balance[4] ?? 0 );
            $map_balances[$address] = $balance;
        }

        $map_addresses = [];
        foreach( $L1_MINERS as $address )
        {
            $l2_address = $L1_API->fetch( '/api/data/' . W8IO_L1_CONTRACT . '/miner_' . $address . '_RewardAddress' );
            if( $l2_address === false )
                $l2_address = $L1_API->fetch( '/api/data/' . W8IO_L1_CONTRACT . '/miner' . $address . 'RewardAddress' );
            if( $l2_address !== false )
            {
                $l2_address = jd( $l2_address );
                $l2_address = reset( $l2_address );
                $map_addresses[$l2_address] = $address;
            }
        }
        
        file_put_contents( $map_addresses_file, je( $map_addresses ) );
        file_put_contents( $map_balances_file, je( $map_balances ) );
    }

    $generators = $RO->getGeneratorsFees( $n, $arg );

    $Q = isset( $showtime ) ? 128 : 80;
    $infos = [];
    $gentotal = 0;
    $feetotal = 0;
    $blktotal = 0;

    foreach( $generators as $generator => $pts )
    {
        $address = $RO->getAddressById( $generator );
        $balance = $map_balances[$map_addresses[$address] ?? 0] ?? 0;
        $gentotal += $balance;

        foreach( $pts as $height => $amount )
        {
            if( !isset( $from ) || $from > $height )
                $from = $height;
            if( !isset( $to ) || $to < $height )
                $to = $height;
        }

        $infos[$generator] = [ 'balance' => $balance, 'pts' => $pts ];
    }

    $fromtime = $RO->getTimestampByHeight( $from );
    $totime = $RO->getTimestampByHeight( $to );

    $q = $n / $Q;
    $qb1 = max( intdiv( (int)$q, 16 ), 5 );
    $qb2 = $qb1 * 4;

    $period = $totime - $fromtime;
    $period = round( $period / 3600 );
    if( $period < 100 )
        $period = $period . ' h';
    else
        $period = round( $period / 24 ) . ' d';

    $totime = date( 'Y.m.d H:i', $totime + $z * 60 );

    if( isset( $showtime ) )
    {
        $highlights = [];
        $hs = intdiv( $from, 10000 );
        $he = intdiv( $to, 10000 );

        if( $hs * 10000 == $from )
            $highlights[] = 0;
        if( 0 && $from === 1 )
            $highlights[] = $Q - 1 - intdiv( $to - 1, $q );
        while( $he !== $hs )
            $highlights[] = $Q - 1 - intdiv( $to - ( ++$hs * 10000 ), $q );

        $topad = str_pad( $to, 7, ' ', STR_PAD_LEFT );
        $periodpad = str_pad( $period, 4, ' ', STR_PAD_LEFT );
        echo "MINERS ( ~ $periodpad ) @ <b>$topad</b> <small>($totime)</small><hr>";
    }
    else
    {
        echo "MINERS ( ~ $period ) @ $to <small>($totime)</small><hr>";
    }

    $generators = $infos;
    uasort( $generators, function( $a, $b ){ return( $a['balance'] < $b['balance'] ? 1 : -1 ); } );

    $n = 0;
    foreach( $generators as $id => $generator )
    {
        $address = $RO->getAddressById( $id );
        $alias = false;
        $padlen = max( 23 - strlen( $alias ), 0 );

        $address = '<a href="' . W8IO_ROOT . "$address\">$address</a>";
        $alias = $alias === false ? ' ' : ' <a href="' . W8IO_ROOT . "$alias\">$alias</a>";
        $alias .= str_pad( '', $padlen );

        $balance = $generator['balance'];
        $percent = str_pad( number_format( gmp_sign( $gentotal ) > 0 ? gmp_intval( gmp_div( gmp_mul( $balance, 100 ), $gentotal ) ) : 0, 2, '.', '' ) . '%', 7, ' ', STR_PAD_LEFT );
        $balance = str_pad( number_format( gmp_intval( gmp_div( $balance, W8IO_L1_BALANCE_DIV ) ), 0, '', "'" ), 10, ' ', STR_PAD_LEFT );

        $pts = $generator['pts'];
        $count = count( $pts );
        $blktotal += $count;

        $matrix = array_fill( 0, $Q, 0 );
        $fee = 0;
        foreach( $pts as $block => $amount )
        {
            $target = max( 0, $Q - 1 - (int)floor( ( $to - $block ) / $q ) );
            $matrix[$target]++;
            $fee += $amount;
        }

        $feetotal += $fee;
        $fee = w8io_amount( $fee, 18, 14 );

        $mxprint = '';
        for( $i = 0; $i < $Q; $i++ )
        {
            $blocks = $matrix[$i];
            if( $blocks === 0 )
                $blocks = '-';
            elseif( $blocks <= $qb1 )
                $blocks = 'o';
            elseif( $blocks <= $qb2 )
                $blocks = 'O';
            else
                $blocks = '<b>O</b>';
            if( isset( $highlights ) && in_array( $i, $highlights ) )
                $blocks = "<a style=\"background-color: #384038;\">$blocks</a>";
            $mxprint .= $blocks;
        }

        if( isset( $showtime ) && $n > 64 )
        {
            $n++;
            continue;
        }

        echo str_pad( ++$n, isset( $showtime ) ? 4 : 3, ' ', STR_PAD_LEFT ) . ") $address $alias $balance $percent  $mxprint $fee ($count)" . PHP_EOL;
    }

    $ntotal = str_pad( isset( $showtime ) ? $n : '', isset( $showtime ) ? 4 : 3, ' ', STR_PAD_LEFT );
    $gentotal = str_pad( number_format( gmp_intval( gmp_div( $gentotal, W8IO_L1_BALANCE_DIV ) ), 0, '', "'" ), 79, ' ', STR_PAD_LEFT );
    $feetotal = w8io_amount( $feetotal, 18, 110 );

    echo "<small style=\"font-size: 50%;\"><br></small><b>$ntotal $gentotal $feetotal</b> ($blktotal)" .  PHP_EOL;

    if( isset( $showtime ) )
        for( $i = $n; $i <= 64; $i++ )
            echo PHP_EOL;
}
else
if( $f === 'data' )
{
    require_once 'include/RO.php';
    $RO = new RO;

    $aid = $RO->getAddressIdByString( $address );
    if( $aid === false )
        exit( 'unknown address' );

    $data = [];
    if( $arg !== false ) // less filter for data keys
    {
        $urio = explode( '/', $urio );
        $arg = $urio[2];
        if( $arg2 === false )
        {
            $key = urldecode( $arg );
            $kid = $RO->getIdByKey( $key );
            if( $kid !== false )
            {
                $value = $RO->getTxKeyValueTypeByAddressKey( $aid, $kid );
                if( $value !== false )
                    [ $data, $lazy ] = w8io_get_data_key( $address, $key, $aid, $kid, PHP_INT_MAX, 100 );
            }

            if( count( $data ) === 0 )
                [ $data, $lazy ] = w8io_get_data( $address, $aid, PHP_INT_MAX, 100, $key );
        }
        else
        {
            $arg2 = $urio[3];
            $arg3 = $arg3 === false ? false : $urio[4];
        }
    }
    else
    {
        [ $data, $lazy ] = w8io_get_data( $address, $aid, PHP_INT_MAX, 100, '' );
    }

    prolog();
    $datauri = '<a href="' . W8IO_ROOT . $address . '/data/';
    $txuri = '<a href="' . W8IO_ROOT . 'tx/';
    echo '<a href="' . W8IO_ROOT . $address . '">' . $address . '</a> &#183; ' . $datauri . '">data</a>';
    if( $arg !== false )
        echo ' &#183; ' . $datauri . $arg . '">' . htmlentities( urldecode( $arg ) ) . '</a>';
    if( $arg2 !== false )
        echo ' &#183; ' . $datauri . $arg2 . '">' . htmlentities( urldecode( $arg2 ) ) . '</a>';
    if( $arg3 !== false )
        echo ' &#183; ' . $datauri . $arg3 . '">' . htmlentities( urldecode( $arg3 ) ) . '</a>';
    echo '<br>' . PHP_EOL . '<pre>{';
    if( isset( $value ) && $value !== false )
    {
        w8io_print_data( $datauri, $txuri, [$data[0]], false );
        echo PHP_EOL . '</pre><br>Changelog:<br><br><pre>{';
        w8io_print_data( $datauri, $txuri, $data, $lazy ?? false, true, $RO );
    }
    else
        w8io_print_data( $datauri, $txuri, $data, $lazy ?? false );
    echo PHP_EOL . '</pre>';
}
else
{
    if( !isset( $RO ) )
    {
        require_once 'include/RO.php';
        $RO = new RO;
    }

    $aid = $RO->getAddressIdByString( $address );

    $where = false;
    $d = 3; // 0 - ?; 1 - i; 2 - o; 3 - io;
    $filter = 0;

    if( !empty( $f ) )
    if( $arg !== false )
    {
        if( $f[0] === 'f' )
        {
            {
                if( $arg === W8IO_ASSET )
                    $arg = 0;
                else
                if( is_numeric( $arg ) )
                {
                    $arg = $RO->getAssetById( $arg );
                    if( $arg === false )
                        exit( 'unknown asset' );
                    exit( header( 'location: ' . W8IO_ROOT . $address . '/f/' . $arg ) );
                }
                else
                {
                    $fasset = $arg;
                    $arg = $RO->getIdByAsset( $arg );
                    if( $arg === false )
                        exit( 'unknown asset' );
                }
            }

            $filter = 1;
            $where = "r5 = $arg";

            if( isset( $f[1] ) )
            {
                if( $f[1] === 'i' )
                {
                    $d = 1;
                }
                else
                if( $f[1] === 'o' )
                {
                    $d = 2;
                }
            }
        }
        else
        if( $f[0] === 't' )
        {
            if( !is_numeric( $arg ) )
                exit( 'unknown type' );

            $filter = 2;
            $where = 'r2 = ' . $arg;

            if( isset( $f[1] ) )
            {
                if( $f[1] === 'i' )
                {
                    $d = 1;
                }
                else
                if( $f[1] === 'o' )
                {
                    $d = 2;
                }
            }
        }
        else if( $aid === false && $f === 'g' )
        {
            if( is_numeric( $arg ) )
            {
                if( $arg === '-1' )
                    $arg = 'failed';
                else
                if( $arg === '-2' )
                    $arg = 'ethereum_transfer';
                else
                {
                    $arg = $RO->getGroupById( (int)$arg );
                    if( $arg === false )
                        exit( 'unknown group' );
                    $first = $arg[0];
                    if( $first === '>' || $first === '<' )
                    {
                        $sep = strpos( $arg, ':' );
                        $asset1 = (int)substr( $arg, 1, $sep - 1 );
                        $asset2 = (int)substr( $arg, $sep + 1 );
                        $asset1 = $asset1 === WAVES_ASSET ? 'WAVES' : $RO->getAssetById( $asset1 );
                        $asset2 = $asset2 === WAVES_ASSET ? 'WAVES' : $RO->getAssetById( $asset2 );
                        $arg = ( $first === '>' ? '1_' : '2_' ) . $asset1 . '_' . $asset2;
                    }
                    else
                    {
                        $args = explode( ':', $arg );
                        $dapp = $RO->getAddressById( $args[0] );
                        $function = $RO->getFunctionById( $args[1] );
                        $arg = $dapp . '_' . $function . '_' . $args[2];
                    }
                }
                exit( header( 'location: ' . W8IO_ROOT . 'txs/g/' . $arg ) );
            }

            if( $arg === 'failed' )
                $arg = FAILED_GROUP;
            else
            if( $arg === 'ethereum_transfer' )
                $arg = ETHEREUM_TRANSFER_GROUP;
            else
            {
                $args = explode( '_', $arg );
                $group = $args[0];
                if( $group === '1' || $group === '2' )
                {
                    if( !isset( $args[1] ) || !isset( $args[2] ) )
                        exit( 'not enough assets' );

                    if( $args[1] === 'WAVES' )
                    {
                        $asset1 = WAVES_ASSET;
                    }
                    else
                    {
                        $asset1 = $RO->getIdByAsset( $args[1] );
                        if( $asset1 === false )
                            exit( 'unknown asset1' );
                    }

                    if( $args[2] === 'WAVES' )
                    {
                        $asset2 = WAVES_ASSET;
                    }
                    else
                    {
                        $asset2 = $RO->getIdByAsset( $args[2] );
                        if( $asset2 === false )
                            exit( 'unknown asset2' );
                    }

                    $group = ( $group === '1' ? '>' : '<' ) . $asset1 . ':' . $asset2; // getGroupExchange
                }
                else
                {
                    $dapp = $RO->getAddressIdByString( $group );
                    if( $dapp === false )
                        exit( 'unknown dapp' );

                    $type = end( $args );
                    if( !is_numeric( $type ) )
                        exit( 'bad type' );

                    $function = substr( $arg, strlen( $group ) + 1, -1 - strlen( $type ) );
                    $function = $RO->getFunctionByName( $function );
                    if( $function === false )
                        exit( 'unknown function' );

                    $group = $dapp . ':' . $function . ':' . $type; // getGroupFunction
                }

                $arg = $RO->getGroupByName( $group );
                if( $arg === false )
                    exit( 'unknown group' );
            }

            $where = "r10 = $arg";
        }
    }
    else
    if( $aid !== false )
    {
        if( $f === 'i' )
        {
            $where = 'io';
            $d = 1;
        }
        else
        if( $f === 'o' )
        {
            $where = 'io';
            $d = 2;
        }
    }

    prolog();
    if( $aid === false )
    {
        echo '<pre>';
        w8io_print_transactions( false, $where, false, 100, $address, $d );
        echo '</pre>';
    }
    else
    {
        if( $aid <= 0 || strlen( $address ) !== 42 )
            $full_address = $RO->getAddressById( $aid );
        else
            $full_address = $address;
        $balance = $RO->getBalanceByAddressId( $aid );

        if( $balance === false )
            $balance = [ 0 => 0 ];

        //$heightTime = $RO->getLastHeightTimestamp();
        //$time = date( 'Y.m.d H:i', $heightTime[1] + $z * 60 );
        //$height = $heightTime[0];

        {
            $full_address_html = $full_address !== $address ? ( ' / <a href="' . W8IO_ROOT . $full_address . '">' . $full_address . '</a>' ) : '';
            //echo "<a href=\"". W8IO_ROOT . $address ."\">$address</a>$full_address @ $height <small>($time) ";
            echo '<a href="' . W8IO_ROOT . $address . '">' . $address . '</a>' . $full_address_html . ' <small>&#183; ';
            echo '<a href="' . W8IO_ROOT . $address . '/i">i</a><a href="' . W8IO_ROOT . $address . '/o">o</a> &#183; ';

            $out = '';
            $data = false;
            for( $t = -16; $t <= 19; ++$t )
            {
                $ti = asset_in( $t );
                $ti = $balance[$ti] ?? 0;
                $to = asset_out( $t );
                $to = $balance[$to] ?? 0;
                if( $ti > 0 || $to > 0 )
                {
                    if( $out !== '' )
                        $out .= ' &#183; ';
                    if( $t === TX_DATA )
                    {
                        $out .= '<a href="' . W8IO_ROOT . $address . '/data">data</a>&#183;';
                        $data = true;
                    }
                    else
                    {
                        if( $t === TX_SMART_ACCOUNT && $data === false )
                            $out .= '<a href="' . W8IO_ROOT . $address . '/data">data</a> &#183; ';
                        $out .= '<a href="' . W8IO_ROOT . $address . '/t/' . $t . '">' . TYPE_STRINGS[$t] . '</a>&#183;';
                    }
                    if( $ti > 0 )
                        $out .= '<a href="' . W8IO_ROOT . $address . '/ti/' . $t . '">i' . $ti . '</a>';
                    if( $to > 0 )
                        $out .= ( $ti > 0 ? '&#183;' : '' ) . '<a href="' . W8IO_ROOT . $address . '/to/' . $t . '">o' . $to . '</a>';
                }
            }
            echo $out . '</small>';
            echo PHP_EOL . PHP_EOL . '<table><tr><td valign="top"><pre>';
        }

        $tickers = [];
        $unlisted = [];

        if( !isset( $balance[0] ) )
            $balance[0] = 0;
        if( $filter === 1 && !isset( $balance[$arg] ) )
            $balance[$arg] = 0;

        $weights = [];
        $prints = [];

        // WAVES
        {
            $asset = W8IO_ASSET;
            $amount = w8io_amount( $balance[0], 18 );
            $furl = W8IO_ROOT . $address . '/f/' . W8IO_ASSET;

            if( $arg === 0 && $filter === 1 )
            {
                echo '<b>' . $amount . ' <a href="' . W8IO_ROOT . 'top/' . W8IO_ASSET . '">' . $asset . '</a></b>';
                echo ' <small><a href="' . W8IO_ROOT . $address . '/fi/' . W8IO_ASSET . '">i</a><a href="' . W8IO_ROOT . $address . '/fo/' . W8IO_ASSET . '">o</a></small>' . PHP_EOL;
                echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
            }
            else
            {
                $weights[WAVES_ASSET] = 10000;
                $prints[WAVES_ASSET] = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];
            }
        }

        if( isset( $balance[WAVES_LEASE_ASSET] ) )
        {
            $amount = $balance[WAVES_LEASE_ASSET] + ( isset( $balance[0] ) ? $balance[0] : 0 );

            if( $balance[0] !== $amount )
            {
                $asset = W8IO_ASSET . ' (MINER)';
                $amount = w8io_amount( $amount, 18 );

                $weights[WAVES_LEASE_ASSET] = 1000;
                $prints[WAVES_LEASE_ASSET] = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];
            }
        }

        foreach( $balance as $asset => $amount )
        {
            if( $amount === '0' && ( $asset !== $arg || $filter !== 1 ) )
                continue;

            if( $asset > 0 )
            {
                $info = $RO->getAssetInfoById( $asset );
                $weight = ord( $info[1] );
                if( $weight === 1 && $arg !== $asset )
                    continue;

                $id = $asset;
                $b = $asset === $arg && $filter === 1;
                $decimals = ord( $info[0] );
                $asset = substr( $info, 2 );
                $amount = w8io_amount( $amount, $decimals );

                $furl = W8IO_ROOT . $address . '/f/' . $id;

                $record = [ 'id' => $id, 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];

                if( $b )
                    $frecord = $record;
                else
                {
                    //if( $weight === -1 )
                        //continue;
                    $weights[$id] = $weight;
                    $prints[$id] = $record;
                }
            }
        }

        if( isset( $frecord ) )
        {
            echo '<b>' . $frecord['amount'] . ' <a href="' . W8IO_ROOT . 'top/' . $frecord['id'] . '">' . $frecord['asset'] . '</a></b>';
            echo ' <small><a href="' . W8IO_ROOT . $address . '/fi/' . $fasset . '">i</a><a href="' . W8IO_ROOT . $address . '/fo/' . $fasset . '">o</a></small>' . PHP_EOL;
            echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
        }

        arsort( $weights );

        foreach( $weights as $asset => $weight )
        {
            if( $weight <= 0 && !isset( $zerotrades ) )
            {
                echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
                $zerotrades = true;
            }

            $record = $prints[$asset];
            echo $record['amount'] . ' <a href="' . $record['furl'] . '">' . $record['asset'] . '</a>' . PHP_EOL;
        }

        if( !isset( $zerotrades ) )
        {
            echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
            $zerotrades = true;
        }

        echo '</pre></td><td valign="top"><pre>';

        if( $f === 'pay' )
        {
            $from = $arg;
            $to = $arg2;

            $incomes = $RO->getLeasingIncomes( $aid, $from, $to );

            if( $incomes !== false )
            for( ;; )
            {
                arsort( $incomes );

                if( $arg3 === 'raw' )
                {
                    echo "raw income ($from .. $to):" . PHP_EOL . PHP_EOL;

                    $raw = [];
                    foreach( $incomes as $a => $p )
                    {
                        $address = $RO->getAddressById( $a );
                        $p = number_format( $p, 14, '.', '' );
                        $raw[$address] = $p;
                    }

                    echo json_encode( $raw, JSON_PRETTY_PRINT );
                    break;
                }

                $percent = (int)$arg3;
                $percent = ( $percent > 0 && $percent < 100 ) ? $percent : 100;

                // waves_fees
                $waves_blocks = 0;
                $waves_fees = 0;

                $start_uid = $RO->db->query( 'SELECT * FROM pts WHERE r1 = ' . w8h2kg( $from ) )->fetchAll();
                if( isset( $start_uid[0][UID] ) )
                    $start_uid = (int)$start_uid[0][UID];
                else
                    break;

                $end_uid = $RO->db->query( 'SELECT * FROM pts WHERE r1 = ' . w8h2kg( $to ) )->fetchAll();
                if( isset( $end_uid[0][UID] ) )
                    $end_uid = (int)$end_uid[0][UID];
                else
                    $end_uid = PHP_INT_MAX;

                $query = $RO->db->query( "SELECT * FROM pts WHERE r0 >= $start_uid AND r0 <= $end_uid AND r4 = $aid AND r2 = 0" );
                foreach( $query as $ts )
                {
                    if( (int)$ts[ASSET] === 0 )
                    {
                        $waves_fees += (int)$ts[AMOUNT];
                        $waves_blocks++;
                    }
                }
                $waves_fees = intval( $waves_fees * $percent / 100 );

                echo "pay ($from .. $to) ($percent %):" . PHP_EOL . PHP_EOL;
                echo w8io_amount( $waves_blocks, 0 ) . ' Blocks' . PHP_EOL;
                echo w8io_amount( $waves_fees, 18 ) . ' ' . W8IO_ASSET . PHP_EOL;

                $payments = [];
                foreach( $incomes as $a => $p )
                    if( $p * $waves_fees > 10000 )
                        $payments[] = $a;

                $reserve = count( $payments );
                $reserve = ( intdiv( $reserve, 100 ) + 1 ) * 100000 + $reserve * 50000 + ( $reserve % 2 ) * 50000;
                $waves_fees -= $reserve * 2;

                echo PHP_EOL;
                $waves = 0;
                $m = 0;
                $n = 0;

                foreach( $payments as $a )
                {
                    $p = $incomes[$a];

                    if( $n === 0 )
                    {
                        $m++;
                        echo '    Mass (' . W8IO_ASSET . ') #$m:' . PHP_EOL;
                        echo "    ------------------------------------------------------------" . PHP_EOL;
                    }
                    $address = $RO->getAddressById( $a );
                    $pay = number_format( $p * $waves_fees / 100000000, 8, '.', '' );
                    $waves += $pay;
                    echo "    $address, $pay" . PHP_EOL;
                    if( ++$n === 100 )
                    {
                        $n = 0;
                        echo "    ------------------------------------------------------------" . PHP_EOL . PHP_EOL;
                    }
                }

                if( $n )
                    echo "    ------------------------------------------------------------" . PHP_EOL . PHP_EOL;

                break;
            }
        }
        else
            w8io_print_transactions( $aid, $where, false, 100, $address, $d );
    }
}

if( !isset( $L ) )
{
    prolog();
}

echo '</pre></td></tr></table>';
echo '<hr><div width="100%" align="right"><pre><small>';
echo "<a href=\"https://github.com/deemru/w8io-ethereum\">github/deemru/w8io-ethereum</a>";
if( file_exists( '.git/FETCH_HEAD' ) )
{
    $rev = file_get_contents( '.git/FETCH_HEAD', false, null, 0, 40 );
    echo "/<a href=\"https://github.com/deemru/w8io-ethereum/commit/$rev\">" . substr( $rev, 0, 7 ) . '</a> ';
}
if( !isset( $showtime ) )
{
    echo PHP_EOL . sprintf( '%.02f ms ', 1000 * ( microtime( true ) - $_SERVER['REQUEST_TIME_FLOAT'] ) );
    echo '<a id="L" href="" style="text-decoration: none;">' . ( $L ? '&#9680' : '&#9681' ) . '</a> ';
    if( defined( 'W8IO_ANALYTICS' ) )
        echo PHP_EOL . PHP_EOL . W8IO_ANALYTICS . ' ';
}
echo '</small></pre></div>
</pre>
    </body>
</html>';
if( isset( $showtime ) )
{
    file_put_contents( $showfile, ob_get_contents() );
    ob_end_clean();
    exit( file_get_contents( $showfile ) );
}
