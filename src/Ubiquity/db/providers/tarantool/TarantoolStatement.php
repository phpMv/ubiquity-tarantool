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
	 * @var \Tarantool\Client\Handler\Handler
	 */
	private $handler;

	/**
	 *
	 * @var SqlQueryResult
	 */
	protected $queryResult;
	
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
		$params=$this->unpackParams($params);
		$request = new ExecuteRequest($this->sql, $params);
		
		return new SqlUpdateResult(
			$this->handler->handle($request)->getBodyField(Keys::SQL_INFO)
		);
	}
	
	protected function executeQuery($params=[]) : SqlQueryResult{
		$params=$this->unpackParams($params);
		$request = new ExecuteRequest($this->sql, $params);
		$response = $this->handler->handle($request);
		return $this->queryResult=new SqlQueryResult(
			$response->getBodyField(Keys::DATA),
			$response->getBodyField(Keys::METADATA)
		);
	}
	
	public function __construct(Client $dbInstance, $sql = null) {
		$this->dbInstance = $dbInstance;
		$this->sql = $sql;
		$this->handler=$dbInstance->getHandler();
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
		$ids=$res->getAutoincrementIds();
		$this->dbInstance->lastInsertId = \is_array($ids)?\current ( $ids ):null;
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
	
	protected function getFields(){
		$metas=$this->queryResult->getMetadata();
		$result=[];
		foreach ($metas as $meta){
			$result[]=\current($meta);
		}
		return $result;
	}

	/**
	 * Returns all the datas in the SqlQueryResult
	 *
	 * @return array
	 */
	public function fetchAll() {
		$datas=$this->queryResult->getData();
		$fields=$this->getFields();
		$result=[];
		foreach ($datas as $row){
			$resultRow=[];
			foreach ($row as $index=>$value){
				$resultRow[$fields[$index]]=$value;
			}
			$result[]=$resultRow;
		}
		return $result;
	}
	
	/**
	 * Returns the first value in result set for a column
	 *
	 * @param int $column
	 * @return mixed
	 */
	public function fetchColumn($column) {
		$column=$column??0;
		$first=\array_values($this->queryResult->getFirst());
		return $first[$column] ?? null;
	}

	/**
	 * Returns all values in result set for a column
	 *
	 * @param int $column
	 * @return array
	 */
	public function fetchAllColumn($column) {
		$result = [ ];
		$column=$column??0;
		$datas = $this->queryResult->getData ();
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
		return $this->queryResult->getFirst ();
	}

	/**
	 * Returns the row count in resultset
	 *
	 * @return number
	 */
	public function rowCount() {
		return $this->queryResult->count ();
	}
}

