<?php
/**
 * 清理MongoDB中超过30天没有清理的用户画像数据 
 */

require('../lib/storage.php');
require('../lib/config.php');

$mongo_cfg=array(
		array('host'=>'192.168.236.139','port'=>27017),
		array('host'=>'192.168.236.139','port'=>27017),
);

 foreach($mongo_cfg as $server)
 { 	
 	$instance=new Clear($server['host'], $server['port']);
 	$instance->run();
 }


 class Clear
 {
 	public $mongo;
 	public $collection; 	
 	public $host;
 	public $port;
 	//一次删除的记录
 	public $once=1;
 	
 	public $redis_cookie2qid;
 	public $redis_qid2cookie;
 	
 	public function  __construct($host,$port)
 	{
 		$this->host=$host;
 		$this->port=$port; 		
 		$this->mongo=new MongoClient(sprintf("mongodb://%s:%s", $host, $port));
 		$this->collection=$this->mongo->selectDB('dop')->selectCollection('behavior'); 		
 		
 		$this->redis_cookie2qid=new RedisStorage('cookie2qid');
 		$this->redis_qid2cookie=new RedisStorage('qid2cookie');		 

 	}
 	
 	public function run()
 	{
 		$min_id=new MongoMinKey();		

 		echo $this->host,"\t",$this->port,"\t",'start',PHP_EOL;
 		do {
 			
 			//要删除的id
 			$clear_ids=array();
 			
 			//找到30天前的数据 		
 			$cursor=$this->collection->find(
 					array('$or'=>
 							array(
 									array('t'=>
 											array('$lt'=>strtotime("-30 day")),'_id'=>array('$gt'=>$min_id)
 									),
 									array('t'=>null)
 							)
 					),
 					array('_id'=>true,'q'=>true)
 			)->sort(array('_id'=>1))->limit($this->once);
 			
		//找不到查询的结果，退出循环
 		if(!$cursor->hasNext())
 			break;
 		
		foreach ($cursor as $r)
		{
			if(isset($r['q']))
			{	
				$qid=$r['q'];
				//删除对应的cookie
				$this->deleteRelatedCookies($qid);			
			}
			
			$clear_ids[]=$r['_id'];
		}
		

			///删除30天前且_id在min_id和max_id的之间的记录，或者t不存在的记录
 			$this->collection->remove(
 						array('_id'=>
 								array(
 									'$in'=>$clear_ids
 								)
 						)	
 					);
 			//将min_id设置成本次查询删除的最大id
 			$min_id=$r['_id'];
 		
 		}while (true);
 		
 		echo $this->host,"\t",$this->port,"\t",'fininsh clear',PHP_EOL;
 	}
 	

 	/**
 	 * 根据qid删除对qid和cookies的对应关系
 	 * @param string $qid
 	 * @return boolean
 	 */
 	public function deleteRelatedCookies($qid)
 	{
 		if(empty($qid))
 			return false;
 		
		$cookies=$this->redis_qid2cookie->sMembers('cookie_'.$qid);
		
		//删除qid2cookies
		$this->redis_qid2cookie->del('cookie_'.$qid);	
		
		//删除cookies2qid	
			
		foreach ($cookies as $cookie)
		{
			list($cookie_name,$cookie_value)=explode(':', $cookie);				 	
			$this->redis_cookie2qid->del('qid_'.$cookie_name.'_'.$cookie_value);			
		}
		return true;
 	}

 }

?>