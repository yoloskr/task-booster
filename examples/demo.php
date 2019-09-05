<?php
/**
 * Created by PhpStorm.
 * User: jiaowenlong
 * Date: 2019-09-05
 * Time: 15:52
 */

require __DIR__.'/vendor/autoload.php';

$booster = new \Booster\Booster(['name'=>'demo']);

$booster->setProvider(range(1,10),3);
$booster->setExecutor(function ($pid, $index,$data){
         printf("executor %d,%d,%d\n",$pid,$index,$data);
},2);

$booster->runAll();