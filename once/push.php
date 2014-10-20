<?php
/**
 * 1,获取原始互众用户数据 c:cid
 * 2,根据q 到 redis，set中查找 对应的cookie名
 * 3,找到全部推送(adeaz::pushMap,adeaz::pushBehavior)
 */

require ('../lib/adeaz.php');

$host='192.168.102.101';
$port=27017;
$mongo=new MongoClient(sprintf("mongodb://%s:%s", $host, $port));
$redis=new Redis();
$redis->connect('192.168.102.101',6379);
$redis->select(2);
$collection=$mongo->selectDB('dop')->selectCollection('behavior');

$last_id=new  MongoMinKey;
$once_select_docs=1000;

echo 'push start!',PHP_EOL;
do {
	//找到互众用户的数据,c=>cid
	$cursor=$collection->find(array('c'=>'cid','_id'=>array('$gt'=>$last_id)))->sort(array('_id'=>1))->limit($once_select_docs);

	if($cursor->count()<=0)
		break;
	
	foreach ( $cursor  as  $doc )
	{
		//得到qid
		if(!isset($doc['q']))
			continue;
		$qid=$doc['q'];
		//根据qid到redis中找对应的cookie
		$cookies=getCookiesByQid($qid);
		if(empty($cookies))
			continue;
		
		foreach($cookies as $cookie)
		{
			list($cookie_name,$cookie_value)=explode(':', $cookie);
			//推送映射关系
			if($cookie_name=='cid')
			{
				Adeaz::pushMap($qid,$cookie_value);
			}
		}
		
		$behavior=$doc;
		unset($behavior['_id']);
		
		//推送用户行为数据
		$updates  = array(
				'$set'=>$behavior
		);
		 
		Adeaz::pushBehavior(array('qid'=>$qid,'data'=>json_encode($updates)));
	}
	
	$last_id=$doc['_id'];	
	echo 'remain '.$cursor->count().'   docs',PHP_EOL;
}while (true);

echo 'push finished!',PHP_EOL;

/**
 * 
 * @param string $qid
 * @return NULL|array
 */
function getCookiesByQid($qid)
{
	if(!isset($qid))
		return NULL;	
	global $redis;	
	$cookies=$redis->sMembers('cookie_'.$qid);
	return $cookies;	
}

// $cursor=$collection->find(array('$or'=>array(,)))->sort(array('_id'=>1))->limit($once_select_docs);


?>