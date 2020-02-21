<?php
namespace Ubiquity\db\providers\tarantool;

use Tarantool\Client\Client;
use Tarantool\Client\Schema\Space;
use Tarantool\Client\Schema\Criteria;
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

	protected function getInstance() {
		return $this->dbInstance;
	}

	public function __construct($dbType = 'default') {
		$this->quote = '"';
	}

	public function fetchAllColumn($statement, array $values = null, string $column = null) {
		if ($statement->query($values)) {
			return $statement->fetchAllColumn($column);
		}
		return false;
	}

	public function lastInsertId() {
		return $this->lastInsertId;
	}

	public function fetchAll($statement, array $values = null, $mode = null) {
		if ($statement->query($values)) {
			return $statement->fetchAll();
		}
		return false;
	}

	public function fetchOne($statement, array $values = null, $mode = null) {
		if ($statement->query($values)) {
			return $statement->fetch($mode);
		}
		return false;
	}

	public static function getAvailableDrivers() {
		return [
			'default'
		];
	}

	public function prepareStatement(string $sql) {
		return new TarantoolStatement($this->getInstance(), $sql);
	}

	public function fetchColumn($statement, array $values = null, int $columnNumber = null) {
		if ($statement->query($values)) {
			return $statement->fetchColumn($columnNumber);
		}
		return false;
	}

	public function getStatement($sql) {
		return new TarantoolStatement($this->getInstance(), $sql);
	}

	public function execute($sql) {
		return $this->getInstance()->executeUpdate($sql);
	}

	public function connect(string $dbType, $dbName, $serverName, string $port, string $user, string $password, array $options) {
		$infoUser = '';
		$opts = '';
		if ($user != null) {
			$infoUser = $user;
			if ($password != null) {
				$infoUser .= ':' . $password;
			}
			$serverName = $infoUser . '@' . $serverName;
		}
		if (\count($options) > 0) {
			$opts = '?' . \http_build_query($options);
		}
		$this->dbInstance = Client::fromDsn($this->getDSN($serverName, $port, $dbName) . $opts);
	}

	public function getDSN(string $serverName, string $port, string $dbName, string $dbType = 'mysql') {
		return 'tcp://' . $serverName . ':' . $port;
	}

	public function bindValueFromStatement($statement, $parameter, $value) {
		return $statement->bindValue($parameter, $value);
	}

	public function query(string $sql) {
		$st = new TarantoolStatement($this->getInstance(), $sql);
		$st->query();
		return $st;
	}

	public function queryAll(string $sql, int $fetchStyle = null) {
		return \iterator_to_array($this->getInstance()->executeQuery($sql));
	}

	public function queryColumn(string $sql, int $columnNumber = null) {
		return \array_values($this->getInstance()
			->executeQuery($sql)
			->getFirst())[$columnNumber];
	}

	public function executeStatement($statement, array $values = null) {
		return $statement->execute($values);
	}

	public function getTablesName() {
		$schema = $this->getInstance()->getSpaceById(Space::VSPACE_ID);
		$rs = $schema->select(Criteria::key([
			512
		])->andGeIterator());
		$result = [];
		foreach ($rs as $item) {
			$result[] = $item[2];
		}
		return $result;
	}

	public function statementRowCount($statement) {
		return $statement->rowCount();
	}

	public function inTransaction() {
		return false;
	}

	public function commit() {}

	public function rollBack() {}

	public function beginTransaction() {}

	public function savePoint($level) {}

	public function releasePoint($level) {}

	public function rollbackPoint($level) {}

	public function nestable() {
		return false;
	}

	public function ping() {
		try {
			$this->getInstance()->ping();
		} catch (\Exception $e) {
			return false;
		}
		return true;
	}

	public function getPrimaryKeys($tableName) {
		$dbInstance = $this->getInstance();
		$idSpace = $this->getSpaceIdByName($tableName);
		$indexesInfos = $dbInstance->getSpaceById(Space::VINDEX_ID)->select(Criteria::key([
			$idSpace
		]));
		$fieldsInfos = $dbInstance->getSpaceById(Space::VSPACE_ID)->select(Criteria::key([
			$idSpace
		]))[0][6];
		$pks = [];
		if ([] !== $fieldsInfos && $firstIndex = \current($indexesInfos)) {
			$indexedfields = $firstIndex[5];
			foreach ($indexedfields as $field) {
				$fieldNum = $field['field'];
				if (isset($fieldsInfos[$fieldNum])) {
					$pks[] = $fieldsInfos[$fieldNum]['name'];
				}
			}
		}
		return $pks;
	}

	public function getFieldsInfos($tableName) {
		$idSpace = $this->getSpaceIdByName($tableName);
		$fieldsInfos = $this->getInstance()
			->getSpaceById(Space::VSPACE_ID)
			->select(Criteria::key([
			$idSpace
		]))[0][6];
		$result = [];
		foreach ($fieldsInfos as $infoField) {
			$result[$infoField['name']] = [
				'Type' => $infoField['type'],
				'Nullable' => $infoField['is_nullable']
			];
		}
		return $result;
	}

	public function getForeignKeys($tableName, $pkName, $dbName = null) {
		$result = [];
		$dbInstance = $this->getInstance();
		$v_space = $dbInstance->getSpaceById(Space::VSPACE_ID);
		$idSpace = $this->getSpaceIdByName($tableName);
		$fieldsInfos = $v_space->select(Criteria::key([
			$idSpace
		]))[0][6];
		$pkFieldNum = $this->getFieldNum($pkName, $fieldsInfos);
		$spacefk = $dbInstance->getSpaceById(356); // _fk_constraint space id
		$foreignKeysInfos = $spacefk->select(Criteria::index(0));
		foreach ($foreignKeysInfos as $fkInfos) {
			if ($fkInfos[2] === $idSpace) { // parent_id same as $tableName id
				foreach ($fkInfos[8] as $parentColIndex => $parent_cols) {
					if ($parent_cols === $pkFieldNum) {
						$fkFieldNum = $fkInfos[7][$parentColIndex]; // child_cols
						$fkSpaceNum = $fkInfos[1]; // child_id
						$fkFieldsInfos = $v_space->select(Criteria::key([
							$fkSpaceNum
						]))[0][6];
						$result[] = [
							'COLUMN_NAME' => $this->getFieldName($fkFieldNum, $fkFieldsInfos),
							'TABLE_NAME' => $this->getSpaceName($fkSpaceNum, $v_space)
						];
					}
				}
			}
		}
		return $result;
	}

	private function getSpaceName($id_space, $v_space) {
		$rs = $v_space->select(Criteria::key([
			$id_space
		]));
		return \current($rs)[2];
	}

	private function getFieldNum($fieldName, $fieldsInfos) {
		foreach ($fieldsInfos as $num => $value) {
			if ($value['name'] === $fieldName) {
				return $num;
			}
		}
		return false;
	}

	private function getFieldName($fieldNum, $fieldsInfos) {
		return $fieldsInfos[$fieldNum]['name'];
	}

	private function getSpaceIdByName(string $spaceName): int {
		return $this->getInstance()
			->getSpace($spaceName)
			->getId();
	}

	public function _optPrepareAndExecute($sql, array $values = null) {
		$statement = $this->_getStatement($sql);
		if ($statement->query($values)) {
			return $statement->fetchAll();
		}
		return false;
	}

	public function getRowNum(string $tableName, string $pkName, string $condition): int {
		return 1;
	}
}
