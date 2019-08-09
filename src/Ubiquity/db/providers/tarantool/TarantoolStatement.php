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

	/**
	 *
	 * @var array of bind parameters=>values
	 */
	protected $params;

	public function __construct(Client $dbInstance, $sql = null) {
		$this->dbInstance = $dbInstance;
		$this->sql = $sql;
	}
	protected $sql;

	/**
	 * Executes an SQL Update statement
	 *
	 * @param mixed ...$params
	 */
	public function execute(...$params) {
		$res = $this->dbInstance->executeUpdate ( $this->sql, ...$params );
		$this->dbInstance->lastInsertId = \current ( $res->getAutoincrementIds () );
		return $res->count ();
	}

	/**
	 * Execute a prapared statement using internal parameters created with bind methods
	 *
	 * @return int
	 */
	public function execPrepared() {
		return $this->execute ( ...$this->params );
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
	 * @param mixed ...$params
	 */
	public function query(...$params) {
		return $this->datas = $this->dbInstance->executeQuery ( $this->sql, ...$params );
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

