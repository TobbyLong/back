<?php
/**
 * adeaz cookie计数统计
 *
 * User: user
 * Date: 2014/10/11
 * Time: 17:53
 */


ini_set('memory_limit', '2G');
set_time_limit(0);

// 批处理分段数量
define('BATCH_SIZE', 4000);

require('../lib/config.php');
require('../lib/storage.php');

// todo: cid文件路径
$fp = fopen('cid.txt', 'r');

// init redis
$redis = new RedisStorage('cookie2qid');

$counter = 0; // 计数器
$cookie_total = 0; // cookie 总数

echo sprintf("%20s%20s\n", 'cookie count', 'exists count');
do
{
	$result = batchGetsAndEncode($fp);
	if ($result['count'] < 1)
	{ // 数量不足的时候返回
		break;
	}

	$cookie_total += $result['count'];

	// redis 自动分组
	$redis->batch($result['keys'], function($rd, $keys) use (&$counter, $cookie_total) {
		$result = $rd->mGet($keys);
		if (is_array($result))
		{
			foreach ($result as $v)
			{
				if ($v !== false)
				{
					++$counter;
				}
			}
		}
		echo sprintf("\r%20d%20d", $cookie_total, $counter);
		usleep(10000); // 休息 0.01秒
	});
	usleep(100000); // 休息 0.1秒
} while(true);

fclose($fp);

echo PHP_EOL;
echo sprintf("------------------------------------------\nexists count: %d", $counter);
echo PHP_EOL;

/**
 * @param     $fp
 * @param int $count
 *
 * @return array [keys => array, count=>xxx]  返回数量，避免再次计算数量
 */
function batchGetsAndEncode(&$fp, $count = BATCH_SIZE)
{
	$data = array(
		'keys' => array(),  // cookie计算出来的key列表
		'count' => 0 // 计数器
	);
	while(($row = fgets($fp)) !== false)
	{
		++$data['count'];
		$data['keys'][] = sprintf('qid_cid_%s', substr(md5(trim($row)), 0, 16));

		if ($data['count'] == $count)
		{ // 每次读取需要的数量
			break;
		}
	}
	return $data;
}