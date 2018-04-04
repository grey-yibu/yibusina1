<?php
/**
 * 启动 10.85.124.210->crontab
 */
include_once dirname(__DIR__) . '/common.php';
include_once dirname(__DIR__) . '/lib/Common/SingletonRedis.php';
include_once dirname(__DIR__) . '/db/Mysql.php';
include_once dirname(__DIR__) . '/function/private_letter_city_data.php';

$date_str = $argv[1] ?: '-7 days';

/************************************************************************/
/** 主运行脚本部分
 *      输出结果到日志文件
 *      减少依赖，每一步骤均可独立运行（低耦合）
 *      耗时预估(大约)：
 *              步骤1(295.097秒)  步骤2(19.643秒)  步骤3(38.13秒)  
 *              步骤4(14.09秒)    步骤5(2.452秒) 
 *      脚本空间占用预估：
 *              10369008 字节 , 约1kb(2018.3.22量)
 *      输出(3*12,以','分割)： 
 *                总爆料量 |     大V用户爆料量    | 普通用户爆料量
 *                过滤总量 |     大V用户过滤量    | 普通用户过滤量
 *            入物料库总量 |   大V用户入物料库量  | 普通用户入物料库量
 *                总曝光量 |     大V用户曝光量    | 普通用户曝光量
 *            总符合红包量 |   大V用户符合红包量  | 普通用户符合红包量
 *            总下发红包量 |   大V用户下发红包量  | 普通用户下发红包量
 *              总下发钱数 |   大V用户 下发钱数   | 普通用户 下发钱数
 *        总下发150红包量  | 大V用户下发150红包量 | 普通用户下发150红包量
 *        总下发100红包量  | 大V用户下发100红包量 | 普通用户下发100红包量
 *        总下发 50红包量  | 大V用户下发 50红包量 | 普通用户下发 50红包量
 *        总下发 20红包量  | 大V用户下发 20红包量 | 普通用户下发 20红包量
 *        总下发 5 红包量  | 大V用户下发 5 红包量 | 普通用户下发 5 红包量
 */

$stat .= step1($date_str);
$stat .= step2($date_str,$PRIVATE_GUONEI_CITY);
$stat .= step3($date_str);
$stat .= step4($date_str);
$stat .= step5($date_str);

//file_put_contents('/data1/sinawap/var/logs/wapcommon/place/newsDig/rewardStat.log', $date_log . "," . $stat . PHP_EOL, FILE_APPEND);
echo date('Ymd', strtotime($date_str)) . "," . $stat . PHP_EOL;

/************************************************************************/
/** 步骤1：  
 *     查询爆料量，以及数据处理
 *     方式：通过日志查询 mid 
 *     /data1/sinawap/var/logs/wapcommon/
 *        place/newsDig/allbaoliao(日期).log
 *           通过内部接口 查询mid 对应  uid 信息并分类
 *     量预估 ： 约10000（1万条,18.3.22） 去重结果
 *     输出(3)： 总爆料量 | 大V用户爆料量 | 普通用户爆料量
 */
function step1($date_str)
{
    $date_log = date('Ymd', strtotime($date_str));
    $at_command = "awk -F '{@}' '{print $4}' /data1/sinawap/var/logs/wapcommon/place/newsDig/allbaoliao{$date_log}.log | grep at |awk -F ',' '{print $1,$2}'";
    $key_command = "awk -F '{@}' '{print $4}' /data1/sinawap/var/logs/wapcommon/place/newsDig/allbaoliao{$date_log}.log | grep key |awk -F ',' '{print $1,$2}'";
    $publisher_command = "awk -F '{@}' '{print $4}' /data1/sinawap/var/logs/wapcommon/place/newsDig/mybaoliao{$date_log}.log|awk -F ',' '{print $1,$2}'";
    @exec($publisher_command, $publisher);
    @exec($at_command, $ats);
    @exec($key_command, $keys);

    $total = array_merge($publisher, $keys, $ats);
    $total = array_unique($total);          // mid uid 去重
    $weibo_count = count($total);
    foreach($total as $key => $value)
    {
        $total[$key] = substr($value,17);   // 去掉前17位 mid (纯净 uid 组)
    }
    $stat .= disVerifyUser_by_uid( $total , $weibo_count);
    return $stat;
}
/************************************************************************/
/** 步骤2：  
 *     查询过滤后量，以及数据处理
 *     方式：通过查Redis库 mid
 *           通过内部接口 查询mid 对应  uid 信息并分类
 *     Bug ：因为微博可能被删除,所以查询可能有丢失
 *           （仅会发生在用mid 对 大V进行判断）
 *           封装函数有保留定义输出 丢失量
 *     输出(3)： 过滤总量 | 大V用户过滤量 | 普通用户过滤量
 */
function step2($date_str,$PRIVATE_GUONEI_CITY)
{
    $date = date('Y-m-d', strtotime($date_str));
    $start_time = strtotime($date . ' 00:00:00');
    $end_time = $start_time + 24 * 3600;
    $redis = SingletonRedis::getInstance('luck', 'lbs');
    $all_filter_count = 0;
    $all_filter_mid = array();
    foreach ($PRIVATE_GUONEI_CITY as $city => $name) {
        $valid_key = 'LBS_NEWS_TIME_FEED_' . $city;
        $valid = $redis->zrangebyscore($valid_key, $start_time, $end_time);
        foreach($valid as $mid)
        {
            $mid = substr($mid,0,16);
            $all_filter_mid[$all_filter_count] = $mid;
            $all_filter_count += 1;
        }
    }

    $stat .= disVerifyUser_by_mid( $all_filter_mid , $all_filter_count );
    return $stat;
}
/************************************************************************/
/** 步骤3：  
 *     查询 入物料库 量，以及数据处理
 *     方式：通过查 mysql 库 uid 
 *           通过内部接口 查询uid 对应  uid 信息并分类
 *     输出(3)： 入物料库总量 | 大V用户入物料库量 | 普通用户入物料库量
 */
function step3($date_str)
{
    $date = date('Y-m-d', strtotime($date_str));
    $conn = new mysql($db_config['w'], $db_config['r']);
    $sql_query_feed = "SELECT uid FROM lbs_news_tag WHERE created_time >= '{$date} 00:00:00' AND created_time <= '{$date} 23:59:59'";
    $uid_array = $conn->getAll($sql_query_feed);

    $feed_count = count($uid_array);
    foreach ($uid_array as $key => $value)
    {
        $uid_array[$key] =  $value['uid'];
    }
    $stat .= disVerifyUser_by_uid( $uid_array , $feed_count );
    return $stat;
}
/************************************************************************/
/** 步骤4：  
 *     查询 曝光数量 量，以及数据处理
 *     方式：通过查 mysql 库 uid 
 *           通过内部接口 查询uid 对应  uid 信息并分类
 *     输出(3)： 总曝光量 | 大V用户曝光量 | 普通用户曝光量
 */
function step4($date_str)
{
    $date = date('Y-m-d', strtotime($date_str));
    $conn = new mysql($db_config['w'], $db_config['r']);
    $sql_query_feed = "SELECT uid  FROM lbs_news_tag WHERE show_times != 0 AND created_time >= '{$date} 00:00:00' AND created_time <= '{$date} 23:59:59'";
    $uid_array = $conn->getAll($sql_query_feed);

    $feed_count = count($uid_array);
    foreach ($uid_array as $key => $value)
    {
        $uid_array[$key] =  $value['uid'];
    }
    $stat .= disVerifyUser_by_uid( $uid_array , $feed_count );
    return $stat;
}
/************************************************************************/
/** 步骤5：  
 *     统计   红包 量，以及数据处理
 *     方式：通过查 mysql 库 lbs_news_reward 表
 *           一次遍历，多变量计数 统计量
 *     输出(3*8)： 
 *            总符合红包量 | 大V用户符合红包量 | 普通用户符合红包量
 *            总下发红包量 | 大V用户下发红包量 | 普通用户下发红包量
 *              总下发钱数 | 大V用户 下发钱数  | 普通用户 下发钱数
 *
 *        总下发150红包量  | 大V用户下发150红包量 | 普通用户下发150红包量
 *        总下发100红包量  | 大V用户下发100红包量 | 普通用户下发100红包量
 *        总下发 50红包量  | 大V用户下发 50红包量 | 普通用户下发 50红包量
 *        总下发 20红包量  | 大V用户下发 20红包量 | 普通用户下发 20红包量
 *        总下发 5 红包量  | 大V用户下发 5 红包量 | 普通用户下发 5 红包量
 */
function step5($date_str)
{
    $date = date('Y-m-d', strtotime($date_str));
    $conn = new mysql($db_config['w'], $db_config['r']);
    $sql_query_reward = "SELECT mid,uid,stat,level FROM lbs_news_reward WHERE created_time >= '{$date} 00:00:00' AND created_time <= '{$date} 23:59:59'";
    $reward_all = $conn->getAll($sql_query_reward);

    foreach ($reward_all  as $key => $value) 
    {
        $url = "http://i2.api.weibo.com/users/show.json?source=". LBS_APPID . "&uid=" . $value['uid'];
        $response = lbs_file_get_contents($url);
        $statuses = json_decode($response, true);

        if($statuses['verified'] == 1)
        {
            $reward_all[ $key ]['type'] = 1;
        }
        else if($statuses['verified'] == 0)
        {
            $reward_all[ $key ]['type'] = 0;
        }  
    }
    // 符合红包 记录变量
    $Fuhe_all_reward = count($reward_all);
    $Fuhe_V_reward = 0;
    $Fuhe_normal_reward = 0;

    // 下发红包 记录变量
    $handed_all_reward = 0;
    $handed_V_reward = 0;
    $handed_normal_reward = 0;

    // 下发钱数 记录变量
    $total_all_reward = 0;
    $total_V_reward = 0;
    $total_normal_reward = 0;

    // 下发钱数具体统计 记录变量
    // 下标从0依次代表  150,100,50,20,5
    $detail_all_reward = array(5=>0,20=>0,50=>0,100=>0,150=>0);
    $detail_V_reward =   array(5=>0,20=>0,50=>0,100=>0,150=>0);
    $detail_normal_reward = array(5=>0,20=>0,50=>0,100=>0,150=>0);

    foreach ( $reward_all as $key => $value )
    {
        if( $value['type'] == 1)    //  判断是否是大V
            $Fuhe_V_reward += 1;
        else                        //  不是大V
            $Fuhe_normal_reward += 1;
            
        if( $value['stat'] )        // 是下发红包
        {
            $handed_all_reward += 1;    //记录 下发红包总数
            if( $value['type'] == 1)    //  判断 下发红包 是否是大V
            {
                $handed_V_reward += 1;
                $total_V_reward += $value['level'];
                $detail_V_reward[$value['level']] += 1;
            }
            else                        //  下发红包 不是大V
            {
                $handed_normal_reward += 1;
                $total_normal_reward += $value['level'];
                $detail_normal_reward[$value['level']] += 1;
            }
            $total_all_reward += $value['level'];
            $detail_all_reward[$value['level']] += 1;
        }
    }
    /*  步骤5 规整化调试输出
    print_r("符合红包:$Fuhe_all_reward | ~V:$Fuhe_V_reward | ~N:$Fuhe_normal_reward \n");
    print_r("下发红包:$handed_all_reward | ~V:$handed_V_reward | ~N:$handed_normal_reward \n");
    print_r("下发钱数 :$total_all_reward | ~V:$total_V_reward | ~N:$total_normal_reward \n");
    print_r("150红包 :$detail_all_reward[150] | ~V:$detail_V_reward[150] | ~N:$detail_normal_reward[150] \n");
    print_r("100红包 :$detail_all_reward[100] | ~V:$detail_V_reward[100] | ~N:$detail_normal_reward[100] \n");
    print_r("50红包 :$detail_all_reward[50] | ~V:$detail_V_reward[50] | ~N:$detail_normal_reward[50] \n");
    print_r("20红包 :$detail_all_reward[20] | ~V:$detail_V_reward[20] | ~N:$detail_normal_reward[20] \n");
    print_r("5红包 :$detail_all_reward[5] | ~V:$detail_V_reward[5] | ~N:$detail_normal_reward[5] \n");
    print_r("\n\n\n");
    */
    $stat .= "$Fuhe_all_reward,$Fuhe_V_reward,$Fuhe_normal_reward,";
    $stat .= "$handed_all_reward,$handed_V_reward,$handed_normal_reward,";
    $stat .= "$total_all_reward,$total_V_reward,$total_normal_reward,";
    $stat .= "$detail_all_reward[150],$detail_V_reward[150],$detail_normal_reward[150],";
    $stat .= "$detail_all_reward[100],$detail_V_reward[100],$detail_normal_reward[100],";
    $stat .= "$detail_all_reward[50],$detail_V_reward[50],$detail_normal_reward[50],";
    $stat .= "$detail_all_reward[20],$detail_V_reward[20],$detail_normal_reward[20],";
    $stat .= "$detail_all_reward[5],$detail_V_reward[5],$detail_normal_reward[5]";
    
    return $stat;
}

/************************************************************************/
/**  
 *     function disVerifyUser_by_uid()
 *     
 *           通过内部接口 查询uid 对应  大V 信息并分类
 */
function disVerifyUser_by_uid( $all_mid , $all_count)
{
    $V_filter_count = 0;
    $Ordinary_filter_count = 0;
    foreach ($all_mid  as $key => $value) 
    {
        $url = "http://i2.api.weibo.com/users/show.json?source=" . LBS_APPID . "&uid=" . $value;
        $response = lbs_file_get_contents($url);
        $statuses = json_decode($response, true);

        if($statuses['verified'] == 1)
        {
            $V_filter_count+=1;
        }
        else if($statuses['verified'] == 0)
        {
            $Ordinary_filter_count += 1;
        }  
    }
    $FindLost_filter_count = $all_count - $Ordinary_filter_count - $V_filter_count;

/*  调试输出  
    echo "all_filter_count:" . $all_count . "\n";
    echo "V_filter_count:" . $V_filter_count . "\n";
    echo "Ordinary_filter_count:" . $Ordinary_filter_count . "\n";
    echo "FindLost_filter_count:" . $FindLost_filter_count . "\n";*/
    
    return "$all_count,$V_filter_count,$Ordinary_filter_count,";
}

/************************************************************************/
/**  
 *     function disVerifyUser_by_mid()
 *     
 *           通过内部接口 查询mid 对应  uid 信息并分类
 */
function disVerifyUser_by_mid( $all_mid , $all_count)
{
    $V_filter_count = 0;
    $Ordinary_filter_count = 0;
    $all_mid = array_chunk($all_mid,50,true);
    foreach ($all_mid  as $key => $value) 
    {
        $comma_separated = implode(",", $all_mid[$key]);
        $url = "http://i2.api.weibo.com/statuses/show_batch.json?source=" . LBS_APPID . "&ids=" . $comma_separated;
        $response = lbs_file_get_contents($url);
        $statuses = json_decode($response, true);
        foreach ($statuses['statuses'] as $zu)
        {
            if($zu['user']['verified'] == 1)
            {
                $V_filter_count+=1;
            }
            else if($zu['user']['verified'] == 0)
            {
                $Ordinary_filter_count += 1;
            }
        }   
    }
    $FindLost_filter_count = $all_count - $Ordinary_filter_count - $V_filter_count;

/*  调试输出   
    echo "all_filter_count:" . $all_count . "\n";
    echo "V_filter_count:" . $V_filter_count . "\n";
    echo "Ordinary_filter_count:" . $Ordinary_filter_count . "\n";
    echo "FindLost_filter_count:" . $FindLost_filter_count . "\n"; */
    
    return "$all_count,$V_filter_count,$Ordinary_filter_count,";
}
/************************************************************************/