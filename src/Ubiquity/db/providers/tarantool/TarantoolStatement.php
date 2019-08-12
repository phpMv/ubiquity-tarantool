<?php

namespace Ubiquity\db\providers\tarantool;

use Tarantool\Client\Client;
use Tarantool\Client\SqlQueryResult;

/**
 * Represents a Tarantool statement (for compatibility reasons with other DBMS).
 * 
 * Ubiquity\db\providers\tarantool$TarantoolStatement
 * This class is part of Ubiquity
 * @author jcheron <myaddressmail@gmail.com>
 * @version 1.0.0
 *
 */
class TarantoolStatement {

	/**
	 *
	 * @var Client
	 */
	private $dbInstance;

	/**
	 *
	 * @var SqlQueryResult
	 */
	protected $datas;
	
	protected $isSelect=false;

	/**
	 *
	 * @var array of bind parameters=>values
	 */
	protected $params;

	protected $sql;
	
	
	public function __construct(Client $dbInstance, $sql = null) {
		$this->dbInstance = $dbInstance;
		$this->sql = $sql;
		$this->type=(\substr ( \strtolower(\trim($sql)), 0, \strlen ( 'select' ) ) === 'select');
	}

	/**
	 * Executes an SQL Update statement
	 *
	 * @param array $params
	 */
	public function execute(array $params=null) {
		if($this->type){
			return $this->query($params);
		}
		if(\is_array($params)){
			$res = $this->dbInstance->executeUpdate ( $this->sql, ...$params );
		}else{
			$res = $this->dbInstance->executeUpdate ( $this->sql);
		}
		$this->dbInstance->lastInsertId = \current ( $res->getAutoincrementIds () );
		return $res->count ();
	}

	/**
	 * Execute a prapared statement using internal parameters created with bind methods
	 *
	 * @return int
	 */
	public function execPrepared() {
		return $this->execute ($this->params );
	}

	/**
	 * Binds a value to a parameter
	 *
	 * @param string $parameter
	 * @param mixed $value
	 */
	public function bindValue($parameter, $value) {
		$this->params [$parameter] = $value;
	}

	/**
	 * Executes an SQL SELECT statement, returning a result set
	 *
	 * @param array $params
	 */
	public function query(array $params=null) {
		if(\is_array($params)){
			return $this->datas = $this->dbInstance->executeQuery ( $this->sql, ...$params );
		}else{
			return $this->datas = $this->dbInstance->executeQuery ( $this->sql);
		}
	}

	/**
	 * Returns all the datas in the SqlQueryResult
	 *
	 * @return array
	 */
	public function fetchAll() {
		return $this->datas->getData ();
	}
	
	/**
	 * Returns the first value in result set for a column
	 *
	 * @param int $column
	 * @return mixed
	 */
	public function fetchColumn($column) {
		if(\count($this->datas)>0){
			return (\current($this->datas)[$column]) ?? null;
		}
		return null;
	}

	/**
	 * Returns all values in result set for a column
	 *
	 * @param int $column
	 * @return array
	 */
	public function fetchAllColumn($column) {
		$result = [ ];
		$datas = $this->datas->getData ();
		foreach ( $datas as $data ) {
			$result [] = $data [$column] ?? null;
		}
		return $result;
	}

	/**
	 * Fetches first row from resultset
	 *
	 * @return array|NULL
	 */
	public function fetch() {
		return $this->datas->getFirst ();
	}

	/**
	 * Returns the row count in resultset
	 *
	 * @return number
	 */
	public function rowCount() {
		return $this->datas->count ();
	}
}

