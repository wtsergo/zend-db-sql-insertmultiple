<?php

namespace Laminas\Db\Sql;

use Laminas\Db\Adapter\ParameterContainer;
use Laminas\Db\Adapter\Platform\PlatformInterface;
use Laminas\Db\Adapter\Driver\DriverInterface;
use Laminas\Db\Exception\InvalidArgumentException;
use Laminas\Db\Sql\AbstractPreparableSql;

class InsertMultiple extends AbstractPreparableSql
{
    /**#@+
     * Constants
     *
     * @const
     */
    const SPECIFICATION_INSERT = 'insert';
    const SPECIFICATION_SELECT = 'select';
    const VALUES_MERGE  = 'merge';
    const VALUES_SET    = 'set';
    /**#@-*/

    /**
     * @var array Specification array
     */
    protected $specifications = array(
        self::SPECIFICATION_INSERT => 'INSERT INTO %1$s (%2$s) VALUES (%3$s)',
        self::SPECIFICATION_SELECT => 'INSERT INTO %1$s %2$s %3$s',
    );

    /**
     * @var string|TableIdentifier
     */
    protected $table            = null;
    protected $columns          = array();

    protected $valueRows        = array();
    protected $rowTemplate      = array();

    /**
     * @var array|Select
     */
    protected $select           = null;

    /**
     * Constructor
     *
     * @param  null|string|TableIdentifier $table
     */
    public function __construct($table = null)
    {
        if ($table) {
            $this->into($table);
        }
    }

    /**
     * Create INTO clause
     *
     * @param  string|TableIdentifier $table
     * @return InsertMultiple
     */
    public function into($table)
    {
        $this->table = $table;
        return $this;
    }

    /**
     * Specify columns
     *
     * @param  array $columns
     * @return InsertMultiple
     */
    public function columns(array $columns)
    {
        $this->columns = array_flip($columns);
        $this->rowTemplate = array_fill_keys($columns, null);
        return $this;
    }

    /**
     * Specify values to insert
     *
     * @param  array|Select $values
     * @param  string $flag one of VALUES_MERGE or VALUES_SET; defaults to VALUES_SET
     * @throws \Laminas\Db\Exception\InvalidArgumentException
     * @return InsertMultiple
     */
    public function values($values, $flag = self::VALUES_SET)
    {
        if ($values instanceof Select) {
            if ($flag == self::VALUES_MERGE) {
                throw new \Laminas\Db\Exception\InvalidArgumentException(
                    'A Laminas\Db\Sql\Select instance cannot be provided with the merge flag'
                );
            }
            $this->select = $values;
            return $this;
        }

        if (!is_array($values)) {
            throw new \Laminas\Db\Exception\InvalidArgumentException(
                'values() expects an array of values or Laminas\Db\Sql\Select instance'
            );
        }
        if ($this->select && $flag == self::VALUES_MERGE) {
            throw new \Laminas\Db\Exception\InvalidArgumentException(
                'An array of values cannot be provided with the merge flag when a Laminas\Db\Sql\Select instance already exists as the value source'
            );
        }
        if ($flag == self::VALUES_SET) {
            $this->valueRows = $values;
        } else {
            $this->valueRows[] = array_merge(
                $this->rowTemplate,
                array_intersect_key($values, $this->columns)
            );
        }
        return $this;
    }

    /**
     * Create INTO SELECT clause
     *
     * @param Select $select
     * @return self
     */
    public function select(Select $select)
    {
        return $this->values($select);
    }

    /**
     * Get raw state
     *
     * @param string $key
     * @return mixed
     */
    public function getRawState($key = null)
    {
        $rawState = array(
            'table' => $this->table,
            'columns' => array_keys($this->columns),
            'values' => array_values($this->valueRows)
        );
        return (isset($key) && array_key_exists($key, $rawState)) ? $rawState[$key] : $rawState;
    }

    protected function processInsert(PlatformInterface $platform, DriverInterface $driver = null, ParameterContainer $parameterContainer = null)
    {
        if ($this->select) {
            return;
        }
        if (!$this->columns) {
            throw new \Laminas\Db\Exception\InvalidArgumentException('values or select should be present');
        }

        $columns = array();
        $values  = array();

        if (empty($this->valueRows)) {
            return '';    //TODO Test that
        }

        $resolvedSubValuesCache = [];

        $pcKey = 'insMulti';
        $pcPos = 0;
        $prepareColumns = true;
        foreach ($this->valueRows as $row) {
            if (!is_array($row)) {
                throw new \Laminas\Db\Exception\InvalidArgumentException('values must be arrays for multi-insertion');
            }
            $subValues = array();
            ksort($row); // Make sure columns always appear in the same order
            foreach($row as $col => $subValue) {
                if ($prepareColumns) {
                    $columns[] = $platform->quoteIdentifier($col);
                }

                if (is_scalar($subValue) && $parameterContainer) {
                    $__pn = $pcKey.$pcPos++;
                    $subValues[] = $driver->formatParameterName($__pn);
                    $parameterContainer->offsetSet($__pn, $subValue);
                } else {
                    // Only use cache for scalar values
                    if (is_scalar($subValue)
                        && array_key_exists($subValueCacheKey = (string)$subValue, $resolvedSubValuesCache[$col] ?? [])
                    ) {
                        $resolvedSubValue = $resolvedSubValuesCache[$col][$subValueCacheKey];
                    } else {
                        $resolvedSubValue = $this->resolveColumnValue(
                            $subValue,
                            $platform,
                            $driver,
                            $parameterContainer
                        );
                    }

                    $subValues[] = $resolvedSubValue;

                    if (is_scalar($subValue)) {
                        $resolvedSubValuesCache[$col][$subValueCacheKey] = $resolvedSubValue;
                    }
                }
            }
            $values[] = implode(', ', $subValues);
            $prepareColumns = false;
        }
        return sprintf(
            $this->specifications[static::SPECIFICATION_INSERT],
            $this->resolveTable($this->table, $platform, $driver, $parameterContainer),
            implode(', ', $columns),
            implode('), (', $values)
        );
    }

    protected function processSelect(PlatformInterface $platform, DriverInterface $driver = null, ParameterContainer $parameterContainer = null)
    {
        if (!$this->select) {
            return;
        }
        $selectSql = $this->processSubSelect($this->select, $platform, $driver, $parameterContainer);

        $columns = array_map(array($platform, 'quoteIdentifier'), array_keys($this->columns));
        $columns = implode(', ', $columns);

        return sprintf(
            $this->specifications[static::SPECIFICATION_SELECT],
            $this->resolveTable($this->table, $platform, $driver, $parameterContainer),
            $columns ? "($columns)" : "",
            $selectSql
        );
    }

    /**
     * Overloading: variable setting
     *
     * Proxies to values, using VALUES_MERGE strategy
     *
     * @param  string $name
     * @param  mixed $value
     * @return InsertMultiple
     */
    public function __set($name, $value)
    {
        $this->columns[$name] = $value;
        return $this;
    }

    /**
     * Overloading: variable unset
     *
     * Proxies to values and columns
     *
     * @param  string $name
     * @throws \Laminas\Db\Exception\InvalidArgumentException
     * @return void
     */
    public function __unset($name)
    {
        if (!isset($this->columns[$name])) {
            throw new \Laminas\Db\Exception\InvalidArgumentException('The key ' . $name . ' was not found in this objects column list');
        }

        unset($this->columns[$name]);
    }

    /**
     * Overloading: variable isset
     *
     * Proxies to columns; does a column of that name exist?
     *
     * @param  string $name
     * @return bool
     */
    public function __isset($name)
    {
        return isset($this->columns[$name]);
    }

    /**
     * Overloading: variable retrieval
     *
     * Retrieves value by column name
     *
     * @param  string $name
     * @throws \Laminas\Db\Exception\InvalidArgumentException
     * @return mixed
     */
    public function __get($name)
    {
        if (!isset($this->columns[$name])) {
            throw new \Laminas\Db\Exception\InvalidArgumentException('The key ' . $name . ' was not found in this objects column list');
        }
        return $this->columns[$name];
    }
}
