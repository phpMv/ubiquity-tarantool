<?php

namespace Ubiquity\db\providers\tarantool;

use Tarantool\Client\Client;
use Tarantool\Client\Schema\Space;
use Tarantool\Client\Schema\Criteria;
use Tarantool\Client\Schema\IndexIds;
use Ubiquity\db\providers\AbstractDbWrapper;

/**
 * Ubiquity\db\providers\tarantool$TarantoolWrapper
 * This class is part of Ubiquity
 *
 * @author jcheron <myaddressmail@gmail.com>
 * @version 1.0.0
 * @property \Tarantool\Client\Client $dbInstance
 */
class TarantoolWrapper extends AbstractDbWrapper {
	protected $transactionLevel = 0;
	protected $lastInsertId;

	public function fetchAllColumn($statement, array $values = null, string $column = null) {
		if ($statement->query ( $values )) {
			return $statement->fetchAllColumn ( $column );
		}
		return false;
	}

	public function lastInsertId() {
		return $this->lastInsertId;
	}

	public function fetchAll($statement, array $values = null, $mode = null) {
		if ($statement->query ( $values )) {
			return $statement->fetchAll ();
		}
		return false;
	}

	public function fetchOne($statement, array $values = null, $mode = null) {
		if ($statement->query ( $values )) {
			return $statement->fetch ( $mode );
		}
		return false;
	}

	public static function getAvailableDrivers() {
		return [ 'default' ];
	}

	public function prepareStatement(string $sql) {
		return new TarantoolStatement ( $this->dbInstance, $sql );
	}

	public function fetchColumn($statement, array $values = null, int $columnNumber = null) {
		if ($statement->query ( $values )) {
			return $statement->fetchColumn ( $columnNumber );
		}
		return false;
	}

	public function getStatement($sql) {
		return new TarantoolStatement ( $this->dbInstance, $sql );
	}

	public function execute($sql) {
		return $this->dbInstance->executeUpdate ( $sql );
	}

	public function connect(string $dbType, $dbName, $serverName, string $port, string $user, string $password, array $options) {
		$this->dbInstance = Client::fromDsn ( $this->getDSN ( $serverName, $port, $dbName ) );
	}

	public function getDSN(string $serverName, string $port, string $dbName, string $dbType = 'mysql') {
		return $dbType . ':dbname=' . $dbName . ';host=' . $serverName . ';charset=UTF8;port=' . $port;
	}

	public function bindValueFromStatement($statement, $parameter, $value) {
		return $statement->bindValue ( $parameter, $value );
	}

	public function query(string $sql) {
		return $this->dbInstance->executeQuery ( $sql );
	}

	public function queryAll(string $sql, int $fetchStyle = null) {
		return $this->dbInstance->executeQuery ( $sql )->getData ();
	}

	public function queryColumn(string $sql, int $columnNumber = null) {
		return $this->dbInstance->executeQuery ( $sql )->getData () [$columnNumber];
	}

	public function executeStatement($statement, array $values = null) {
		return $statement->execute ( $values );
	}

	public function getTablesName() {
		$schema = $this->dbInstance->getSpaceById ( Space::VSPACE_ID );
		$rs = $schema->select ( Criteria::index ( IndexIds::SPACE_NAME ) );

		$result = [ ];
		foreach ( $rs as $item ) {
			if (\substr ( $item [2], 0, \strlen ( '_' ) ) !== '_') {
				$result [] = $item [2];
			}
		}
		return $result;
	}

	public function statementRowCount($statement) {
		return $statement->rowCount ();
	}

	public function inTransaction() {
		return false;
	}

	public function commit() {
	}

	public function rollBack() {
	}

	public function beginTransaction() {
	}

	public function savePoint($level) {
	}

	public function releasePoint($level) {
	}

	public function rollbackPoint($level) {
	}

	public function nestable() {
		return false;
	}

	public function ping() {
		try {
			$this->dbInstance->ping ();
		} catch ( \Exception $e ) {
			return false;
		}
		return true;
	}

	public function getPrimaryKeys($tableName) {
		$idSpace = $this->getSpaceIdByName ( $tableName );
		$indexesInfos = $this->dbInstance->getSpaceById ( Space::VINDEX_ID )->select ( Criteria::key ( [ $idSpace ] ) );
		$fieldsInfos = $this->dbInstance->getSpaceById ( Space::VSPACE_ID )->select ( Criteria::key ( [ $idSpace ] ) ) [0] [6];
		$pks = [ ];
		if ([ ] !== $fieldsInfos && $firstIndex = \current ( $indexesInfos )) {
			$indexedfields = $firstIndex [5];
			foreach ( $indexedfields as $field ) {
				$fieldNum = $field ['field'];
				if (isset ( $fieldsInfos [$fieldNum] )) {
					$pks [] = $fieldsInfos [$fieldNum] ['name'];
				}
			}
		}
		return $pks;
	}

	private function getSpaceIdByName(string $spaceName): int {
		$schema = $this->dbInstance->getSpaceById ( Space::VSPACE_ID );
		$data = $schema->select ( Criteria::key ( [ $spaceName ] )->andIndex ( IndexIds::SPACE_NAME ) );
		if ([ ] === $data) {
			throw new \Exception ( "unknownSpace($spaceName)" );
		}
		return $data [0] [0];
	}
}