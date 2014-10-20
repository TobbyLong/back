<?php

$m  = new  MongoClient ();
$collection  =  $m -> selectDB ( "foo" )-> selectCollection ( "bar" );

$collection -> insert (array(  "x"  =>  "y"  ));
$collection -> insert (array(  "x"  =>  "y"  ));

$cursor  =  $collection -> find ();
$r1  =  $cursor -> getNext ();



echo  $r1 [ "_id" ] .  "\n" ;


?> 
