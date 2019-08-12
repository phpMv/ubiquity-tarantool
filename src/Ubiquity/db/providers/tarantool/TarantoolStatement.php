<?php

namespace Ubiquity\db\providers\tarantool;

use Tarantool\Client\Client;
use Tarantool\Client\SqlQueryResult;
use Tarantool\Client\Request\ExecuteRequest;
use Tarantool\Client\SqlUpdateResult;
use Tarantool\Client\Keys;

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
	protected $params=[];

	protected $sql;
	
	
	protected function unpackParams($params){
		
		if((\array_keys ( $params ) !== \range ( 0, \count ( $params ) - 1 ))){
			$result=[];
			foreach ($params as $k=>$param){
				$result[]=[':'.$k=>$param];
			}
			return $result;
		}
		return $params;
	}
	
	protected function executeUpdate($params=[]): SqlUpdateResult{
		$request = new ExecuteRequest($this->sql, $params);
		
		return new SqlUpdateResult(
			$this->dbInstance->getHandler()->handle($request)->getBodyField(Keys::SQL_INFO)
		);
	}
	
	protected function executeQuery($params=[]) : SqlQueryResult{
		$request = new ExecuteRequest($this->sql, $params);
		$response = $this->dbInstance->getHandler()->handle($request);
		
		return new SqlQueryResult(
			$response->getBodyField(Keys::DATA),
			$response->getBodyField(Keys::METADATA)
		);
	}
	
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
		$params=$params??$this->params;
		if($this->type){
			return $this->executeQuery($params);
		}
		$res = $this->executeUpdate($params);
		$this->dbInstance->lastInsertId = \current ( $res->getAutoincrementIds () );
		return $res->count ();
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
		$params=$params??$this->params;
		return $this->executeQuery($params);
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

