<?php
/**
 * MongoDb session handler with locking
 */
class Cm_Mongo_Model_Mysql4_Session extends Mage_Core_Model_Mysql4_Session
{
    const BREAK_AFTER = 15;
    const FAIL_AFTER = 20;

    /** @var bool */
    protected $_useMongo;

    /** @var Mongo */
    protected $_mongo;

    /** @var MongoDb */
    protected $_db;

    /** @var string */
    protected $_dbName;

    /** @var MongoCollection */
    protected $_coll;

    /**
     * Because this isn't passed along.. :(
     *
     * @return string
     */
    public function getSessionSavePath()
    {
        if (Mage::isInstalled() && $sessionSavePath = Mage::getConfig()->getNode(Mage_Core_Model_Session_Abstract::XML_NODE_SESSION_SAVE_PATH)) {
            return (string) $sessionSavePath;
        }
        return Mage::getBaseDir('session');
    }

    public function __construct()
    {
        $server = $this->getSessionSavePath();
        $this->_useMongo = (strpos($server, 'mongo://') === 0);
        if($this->_useMongo) {
          $this->_dbName = basename($server);
          if($this->_dbName) {
            $this->_mongo = new Mongo($server, array(
              'connect' => FALSE,
              'persist' => 'sessions_'.crc32($server),
              'timeout' => 10000, // 10 seconds
            ));
            return; // Success. All other failures fall through to parent
          } else {
            Mage::log('Mongo server string must specify db name.');
            $this->_useMongo = FALSE;
          }
        }
        parent::__construct();
    }

    /**
     * Check DB connection
     *
     * @return bool
     */
    public function hasConnection()
    {
        if( ! $this->_useMongo) return parent::hasConnection();

        try {
            $this->_mongo->connected or $this->_mongo->connect();
            $this->_db = $this->_mongo->selectDB($this->_dbName);
            $this->_coll = $this->_db->selectCollection('sessions');
            $this->_coll->ensureIndex(array('expires' => 1));
            return TRUE;
        }
        catch(Exception $e) {
            Mage::logException($e);
            $this->_mongo = NULL;
            $this->_useMongo = FALSE;
        }
        return FALSE;
    }

    /**
     * Fetch session data
     *
     * @param string $sessId
     * @return string
     */
    public function read($sessId)
    {
        if( ! $this->_useMongo) return parent::read($sessId);

        // Get lock on session. Increment the "lock" field and if the new value is 1, we have the lock.
        // If the new value is exactly BREAK_AFTER then we also have the lock and have broken the
        // lock for the previous process.
        $tries = 0;
        while(1) {
            $result = $this->_db->command(array(
                'findAndModify' => $this->_coll->getName(),
                'query' => array('_id' => $sessId),
                'fields' => array('lock' => 1, 'data' => 1),
                'update' => array('$inc' => array('lock' => 1)),
                'new'    => TRUE,
                'upsert' => TRUE,
            ));
            if( empty($result['ok']) ) {
                $message = isset($result['errmsg']) ? $result['errmsg'] : 'Error: '.json_encode($result);
                $code = isset($result['errno']) ? $result['errno'] : 0;
                Mage::log("FindAndModify command failed: $message ($code)");
                return '';
            }
            $doc = $result['value'];

            // If we got the lock, update with our pid and reset lock and expiration
            if($doc['lock'] == 1 || $doc['lock'] == self::BREAK_AFTER) {
                $this->_coll->update(array(
                    '_id' => $sessId
                ), array(
                  '$set' => array(
                      'pid' => getmypid(),
                      'lock' => 1,
                      'expires' => time() + $this->getLifeTime(),
                  )
                ));
                return isset($doc['data']) ? $doc['data'] : '';
            }
            if(++$tries >= self::FAIL_AFTER) {
                return '';
            }
            sleep(1);
        }
    }

    /**
     * Update session
     *
     * @param string $sessId
     * @param string $sessData
     * @return boolean
     */
    public function write($sessId, $sessData)
    {
        if( ! $this->_useMongo) return parent::write($sessId, $sessData);

        // If we lost our lock on the session we should not overwrite it.
        // It should always exist since the read callback created it.
        $this->_db->selectCollection('sessions')->update(array(
            '_id' => $sessId,
            'pid' => getmypid(),
        ), array(
            '$set' => array(
                'data' => $sessData,
                'lock' => 0,  // Unlocks since next lock attempt will get '1'
            )
        ), array('multiple' => FALSE));
        return TRUE;
    }

    /**
     * Destroy session
     *
     * @param string $sessId
     * @return boolean
     */
    public function destroy($sessId)
    {
        if( ! $this->_useMongo) return parent::destroy($sessId);

        $this->_coll->remove(array('_id' => $sessId));
        return true;
    }

    /**
     * Garbage collection
     *
     * @param int $sessMaxLifeTime ignored
     * @return boolean
     */
    public function gc($sessMaxLifeTime)
    {
        if( ! $this->_useMongo) return parent::gc($sessMaxLifeTime);

        if ($this->_automaticCleaningFactor > 0) {
            if ($this->_automaticCleaningFactor == 1 ||
                rand(1, $this->_automaticCleaningFactor)==1) {
                $this->_coll->remove(array('expires' => array('$lt' => time())), array('multiple' => TRUE));
            }
        }
        return true;
    }
}
