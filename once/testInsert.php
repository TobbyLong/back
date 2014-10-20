<?php
if($argc==3)
{
	$host=$argv[1];
	$port=$argv[2];
}
else
{
	echo 'command error！';
	exit(-1);
}

$mongo=new MongoClient(sprintf("mongodb://%s:%s", $host, $port));
$collection=$mongo->selectDB('dop')->selectCollection('behavior');

for($i=0;$i<111;$i++)
{
	$rand_time=mt_rand(30, 35);
	$collection->insert(array('t'=>strtotime("-$rand_time day"),'q'=>'00542006a09148a'));
}

for($i=0;$i<222;$i++)
{
	$collection->insert(array('random'=>mt_rand(0, 100)));
}

for($i=0;$i<333;$i++)
{
	$rand_time=mt_rand(10, 15);
	$collection->insert(array('t'=>strtotime("-$rand_time day")));
	
}

for ($i=0;$i<444;$i++)
{
	$rand_time=mt_rand(0, 10000000000);
	$collection->insert(array('q'=>$rand_time));
}