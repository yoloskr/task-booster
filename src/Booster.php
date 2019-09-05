<?php
/**
 * Created by PhpStorm.
 * User: jiaowenlong
 * Date: 2019-09-05
 * Time: 12:02
 */

namespace Booster;


use Booster\Contracts\Executor;
use Booster\Contracts\TaskProvider;

class Booster
{
    /**
     * Version
     * @var string
     */
    const VERSION = '0.0.1';


    /**
     * Provider process type
     */
    const PROVIDER_PROCESS_TYPE  = 1;

    /**
     * Executor process type
     */
    const EXECUTOR_PROCESS_TYPE  = 2;


    /**
     * Normal message
     */
    const MSG_TYPE_NORMAL = 1;

    /**
     * End message
     */
    const MSG_TYPE_END    = 99;

    /**
     * Pid store file path
     *
     * @val string
     */
    public $pid_file      = '/tmp/';

    /**
     * Total task
     *
     * @var int
     */
    public static $total_task = 0;

    /**
     * Daemon
     *
     * @var bool
     */
    public static $daemon  = false;

    const SHMID = '0x7777';

    /**
     * Config
     *
     * @var array
     */
    protected $config = [
        'name'=>'booster'
    ];

    /**
     * Child
     *
     * @var array
     */
    protected $childs = [];

    /**
     * Executor process
     *
     * @var array
     */
    protected $executor;

    /**
     * Executor process default value
     *
     * @var int
     */
    protected $executor_num = 1;

    /**
     * Executor process
     * @var array
     */
    protected $executor_process = [];

    /**
     * Provider process
     *
     * @var callable|array|TaskProvider
     */
    protected $provider;

    /**
     * Provider process default value
     *
     * @var
     */
    protected $provider_num = 1;

    /**
     *  Provider process
     * @var array
     */
    protected $provider_process = [];

    /**
     * Main process quit flag
     *
     * @var bool
     */
    protected $quit     = false;

    /**
     * System V message queue handler
     *
     * @var resource
     */
    protected $queue;

    public function __construct($config = [])
    {
        $this->setConfig($config);
    }

    /**
     * start
     */
    public function runAll()
    {
        $this->checkEnv();
        $this->checkStatus();
        $this->prepare();
        $this->createProviderProcess();
        $this->createExecutorProcess();

        //Waiting signal, dispatch signal
        while (!$this->quit){
               sleep(10);
               pcntl_signal_dispatch();
        }

        $this->ending();
    }

    /**
     * Check if the extension is installed
     */
    protected function checkEnv()
    {
        if (!(extension_loaded('pcntl')
            && extension_loaded('posix')
            && extension_loaded('sysvmsg')
            && extension_loaded('sysvshm'))
        ){
           printf('Environment needs to install extension pcntl,posix,sysvmsg,sysvshm');
           exit();
        }
    }


    /**
     * Set config
     *
     * @param $config
     */
    public function setConfig($config)
    {
        $this->config = array_merge($this->config, $config);
    }

    /**
     * Check status
     */
    public function checkStatus()
    {
        if(file_exists($this->getPidFile())){
            printf("booster is running,pid is %d\n",file_get_contents($this->getPidFile()));
            exit(0);
        }
    }

    /**
     * Prepare
     */
    protected function prepare()
    {
        $this->savePid();
        $this->setProcessTitle('main');
        $this->registerSignal();
        $this->queue = msg_get_queue(posix_getpid());
    }



    /**
     * Get pid file
     *
     * @return string
     */
    public function getPidFile()
    {
        return $this->pid_file.$this->config['name'];
    }

    /**
     * Get config
     *
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * Get queue
     *
     * @return resource
     */
    protected function getQueue()
    {
        return $this->queue;
    }

    /**
     * Save pid to file
     */
    protected function savePid()
    {
         $pid = posix_getpid();
         file_put_contents($this->getPidFile(),$pid);
    }

    /**
     *  Set process title
     *
     * @param  string $type
     * @return void
     */
    protected function setProcessTitle($type)
    {
        cli_set_process_title($this->config['name'] . ': ' . $type);

    }


    /**
     *  Main exit
     */
    public function mainExit()
    {
        $this->quit = true;
    }

    /**
     * Judge if need to restart
     *
     * @param  int $status
     * @return bool
     */
    private function isNeedRestart($status)
    {
        return (!pcntl_wifexited($status) || pcntl_wexitstatus($status)) && !$this->quit && $this->config['restart'];
    }


    /**
     *  Child exit
     */
    private function childExit()
    {
        while ( ($pid = pcntl_wait($status, WNOHANG)) > 0) {

            // if exited is consumer
            if ( ($index = array_search($pid, $this->executor_process)) !== false) {
                if ($this->isNeedRestart($status)) {
                    $this->createExecutorProcess($index);
                } else {
                    unset($this->executor_process[$index]);
                    if (!$this->executor_process) {
                        // no consumers, main quit
                        $this->mainExit();
                    }
                }

                continue;
            }

            // if exited is producer
            if ( ($index = array_search($pid, $this->provider_process)) !== false) {
                if ($this->isNeedRestart($status)) {
                    $this->createProviderProcess($index);
                } else {
                    unset($this->provider_process[$index]);
                    if (!$this->provider_process && $this->queue) {
                        for ($i = 0; $i < $this->executor_num; ++$i) {
                            msg_send($this->queue, self::MSG_TYPE_END, NULL);
                        }
                    }
                }
            }
        }
    }


    public function registerSignal()
    {
        pcntl_signal(SIGINT,  [$this, 'mainExit']);
        pcntl_signal(SIGTERM, [$this, 'mainExit']);
        pcntl_signal(SIGCHLD, [$this, 'childExit']);

    }

    public function setProvider($provider, $num)
    {
          $this->provider     = $provider;
          $this->provider_num = $num;
    }

    public function setExecutor($executor, $num)
    {
         $this->executor     = $executor;
         $this->executor_num = $num;
    }

    public function createProviderProcess()
    {
        for ($i = 0; $i < $this->provider_num; ++$i) {
            try{
                $this->generateSingleProviderProcess($i);
            }catch (\Exception $e){
                var_dump($e->getMessage());
            }

        }
    }

    protected function generateSingleProviderProcess($index)
    {

        if(($pid = pcntl_fork())==-1){

        }
        if($pid == 0){
            // reset signal
            pcntl_signal(SIGINT, SIG_DFL);
            pcntl_signal(SIGTERM, SIG_DFL);

            // set producer process title
            $this->setProcessTitle('provider');

            $currentPid = posix_getpid();
            while (true){
                $data = false;
                if(is_callable($this->provider)){
                    $data = call_user_func($this->provider, $currentPid, $index);
                }else if($this->provider instanceof TaskProvider){
                    $data = $this->provider->getData();
                }else if(is_array($this->provider)){
                    $data = $this->provider ? array_shift($this->provider) : false;
                }

                if($data === false){
                    break;
                }

                if($data != NULL){
                    //todo 待完善
                    if(is_array($data)){
                        //send data to queue
                        foreach ($data as $value){
                            $this->sendDataToQueue($value);
                        }
                        //record total task
                        self::$total_task += count($data);
                    }else{
                        //send data to queue
                        $this->sendDataToQueue($data);
                        //record total task
                        self::$total_task ++;
                    }

                }
            }

            for ($i=0; $i<$this->provider_num; $i++){
              //  msg_send($this->queue, self::MSG_TYPE_END, NULL);
            }

            exit(0);
        }

        $this->provider_process[$index] = $pid;

    }


    /**
     * Create executor process
     */
    public function createExecutorProcess()
    {
        for ($i = 0; $i < $this->executor_num; ++$i) {
            $this->generateSingleExecutorProcess($i);
        }
    }

    /**
     * Generate single executor process
     *
     * @param $index
     */
    protected function generateSingleExecutorProcess($index)
    {
        //fork process
        if(($pid =  pcntl_fork()) == -1){
            printf('fork fail');
            exit();
        }else if($pid == 0){
            // reset signal
            pcntl_signal(SIGINT, SIG_DFL);
            pcntl_signal(SIGTERM, SIG_DFL);

            // set executor process title
            $this->setProcessTitle('executor');

            $currentPid = posix_getpid();
            // executor loop
            while (true) {

                if (msg_receive($this->queue, 0, $msgtype, 8192, $data, true, 0, $errcode)) {
                } else {
                }

                if ($msgtype === self::MSG_TYPE_NORMAL) {
                    if(is_callable($this->executor)){
                        $data = call_user_func($this->executor, $currentPid, $index,$data);
                    }else if($this->executor instanceof Executor){
                        $data = $this->provider->execute();
                    }
                } else {
                    break;
                }
            }

            exit(0);
        }

        $this->executor_process[$index] = $pid;

    }

    /**
     * Send data to queue
     * @param $data
     * @return bool
     */
    protected function sendDataToQueue($data)
    {
       return  msg_send($this->queue, self::MSG_TYPE_NORMAL, $data, true, true, $errcode);
    }

    /**
     * ending handle
     */
    public function ending()
    {
        //Kill the child process before the main process exits
        foreach ($this->executor as $pid=>$executor){
            posix_kill($pid, SIGTERM);
        }

        // remove message queue
        if ($this->queue) {
            msg_remove_queue($this->queue);
        }

        // unlink pid file
        if (file_exists($this->getPidFile())) {
            unlink($this->getPidFile());
        }

    }
}