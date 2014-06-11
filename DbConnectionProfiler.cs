using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;

namespace DbConnectionProfiler
{
    //REF: https://github.com/MiniProfiler/dotnet
    public interface IDbProfiler
    {
        bool IsActive { get; set; }
        void ExecuteStart(ProfiledDbCommand profiledDbCommand, SqlExecuteType executeType);
        void OnError(ProfiledDbCommand profiledDbCommand, SqlExecuteType nonQuery, Exception exception);
        void ReaderFinish(ProfiledDbDataReader profiledDbDataReader);
        void ExecuteFinish(ProfiledDbCommand profiledDbCommand, SqlExecuteType executeType, DbDataReader reader);
    }

    public class DbProfiler : IDbProfiler
    {
        private static IDbProfiler _current;

        public static IDbProfiler Current
        {
            get { return _current ?? (_current = new DbProfiler { IsActive = true }); }
        }

        public bool IsActive { get; set; }

        public void ExecuteStart(ProfiledDbCommand profiledDbCommand, SqlExecuteType executeType)
        {

        }

        public void OnError(ProfiledDbCommand profiledDbCommand, SqlExecuteType nonQuery, Exception exception)
        {

        }

        public void ReaderFinish(ProfiledDbDataReader profiledDbDataReader)
        {

        }

        public void ExecuteFinish(ProfiledDbCommand profiledDbCommand, SqlExecuteType executeType, DbDataReader reader)
        {
#if DEBUG
            System.Diagnostics.Debug.Print(GetCommandText(profiledDbCommand));
#endif
        }

        public static IDbConnection Wrap(DbConnection conn)
        {
#if DEBUG
            return new ProfiledDbConnection(conn, Current);
#else
            return conn;
#endif
        }

        private static string GetCommandText(IDbCommand cmd)
        {
            var prms = cmd.Parameters.Cast<SqlParameter>().ToList();
            var builder = new StringBuilder();

            // StoredProcedure
            if (cmd.CommandType == CommandType.StoredProcedure)
            {
                builder.Append("EXEC " + cmd.CommandText);
                for (var i = 0; i < prms.Count; i++)
                {
                    if (prms[i].Direction != ParameterDirection.Input && prms[i].Direction != ParameterDirection.InputOutput)
                        continue;

                    if (i > 0)
                        builder.Append(',');

                    builder.AppendFormat(" @{0}={1}", prms[i].ParameterName, GetParameterValue(prms[i]));
                }
            }
            // Sql
            else
            {
                var sql = cmd.CommandText;
                for (var i = 0; i < prms.Count; i++)
                {
                    sql = sql.Replace(prms[i].ParameterName, GetParameterValue(prms[i]));
                }
                builder.Append(sql);
            }
            return builder.ToString();
        }

        private static string GetParameterValue(SqlParameter prm)
        {
            if ((prm.SqlValue is INullable) && ((INullable)prm.SqlValue).IsNull)
            {
                return "NULL";
            }
            if (prm.SqlValue is SqlString || prm.SqlValue is SqlDateTime)
            {
                return "'" + prm.Value + "'";
            }
            return prm.Value + "";
        }
    };

    /// <summary>
    /// Wraps a database connection, allowing SQL execution timings to be collected when a DbProfiler session is started.
    /// </summary>
    [System.ComponentModel.DesignerCategory("")]
    public class ProfiledDbConnection : DbConnection, ICloneable
    {
        /// <summary>
        /// Gets the underlying, real database connection to your database provider.
        /// </summary>
        public DbConnection InnerConnection { get; private set; }

        /// <summary>
        /// Gets the current profiler instance; could be null.
        /// </summary>
        public IDbProfiler DbProfiler { get; private set; }

        /// <summary>
        /// Initialises a new instance of the <see cref="ProfiledDbConnection"/> class. 
        /// Returns a new <see cref="ProfiledDbConnection"/> that wraps <paramref name="connection"/>, 
        /// providing query execution profiling. If profiler is null, no profiling will occur.
        /// </summary>
        /// <param name="connection">
        /// <c>Your provider-specific flavour of connection, e.g. SqlConnection, OracleConnection</c>
        /// </param>
        /// <param name="profiler">
        /// The currently started DbProfiler or null.
        /// </param>
        public ProfiledDbConnection(DbConnection connection, IDbProfiler profiler)
        {
            if (connection == null) throw new ArgumentNullException("connection");

            InnerConnection = connection;
            InnerConnection.StateChange += StateChangeHandler;

            if (profiler != null)
            {
                DbProfiler = profiler;
            }
        }

        /// <summary>
        /// Gets the wrapped connection.
        /// </summary>
        public DbConnection WrappedConnection
        {
            get { return InnerConnection; }
        }

        /// <summary>
        /// Gets a value indicating whether events can be raised.
        /// </summary>
        protected override bool CanRaiseEvents
        {
            get { return true; }
        }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        public override string ConnectionString
        {
            get { return InnerConnection.ConnectionString; }
            set { InnerConnection.ConnectionString = value; }
        }

        /// <summary>
        /// Gets the connection timeout.
        /// </summary>
        public override int ConnectionTimeout
        {
            get { return InnerConnection.ConnectionTimeout; }
        }

        /// <summary>
        /// Gets the database.
        /// </summary>
        public override string Database
        {
            get { return InnerConnection.Database; }
        }

        /// <summary>
        /// Gets the data source.
        /// </summary>
        public override string DataSource
        {
            get { return InnerConnection.DataSource; }
        }

        /// <summary>
        /// Gets the server version.
        /// </summary>
        public override string ServerVersion
        {
            get { return InnerConnection.ServerVersion; }
        }

        /// <summary>
        /// Gets the state.
        /// </summary>
        public override ConnectionState State
        {
            get { return InnerConnection.State; }
        }

        /// <summary>
        /// change the database.
        /// </summary>
        /// <param name="databaseName">The new database name.</param>
        public override void ChangeDatabase(string databaseName)
        {
            InnerConnection.ChangeDatabase(databaseName);
        }

        /// <summary>
        /// close the connection.
        /// </summary>
        public override void Close()
        {
            InnerConnection.Close();
        }

        /// <summary>
        /// enlist the transaction.
        /// </summary>
        /// <param name="transaction">The transaction.</param>
        public override void EnlistTransaction(System.Transactions.Transaction transaction)
        {
            InnerConnection.EnlistTransaction(transaction);
        }

        /// <summary>
        /// get the schema.
        /// </summary>
        /// <returns>The <see cref="DataTable"/>.</returns>
        public override DataTable GetSchema()
        {
            return InnerConnection.GetSchema();
        }

        /// <summary>
        /// get the schema.
        /// </summary>
        /// <param name="collectionName">The collection name.</param>
        /// <returns>The <see cref="DataTable"/>.</returns>
        public override DataTable GetSchema(string collectionName)
        {
            return InnerConnection.GetSchema(collectionName);
        }

        /// <summary>
        /// get the schema.
        /// </summary>
        /// <param name="collectionName">The collection name.</param>
        /// <param name="restrictionValues">The restriction values.</param>
        /// <returns>The <see cref="DataTable"/>.</returns>
        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            return InnerConnection.GetSchema(collectionName, restrictionValues);
        }

        /// <summary>
        /// open the connection
        /// </summary>
        public override void Open()
        {
            InnerConnection.Open();
        }

        /// <summary>
        /// begin the database transaction.
        /// </summary>
        /// <param name="isolationLevel">The isolation level.</param>
        /// <returns>The <see cref="DbTransaction"/>.</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return new ProfiledDbTransaction(InnerConnection.BeginTransaction(isolationLevel), this);
        }

        /// <summary>
        /// create the database command.
        /// </summary>
        /// <returns>The <see cref="DbCommand"/>.</returns>
        protected override DbCommand CreateDbCommand()
        {
            return new ProfiledDbCommand(InnerConnection.CreateCommand(), this, DbProfiler);
        }

        /// <summary>
        /// dispose the underlying connection.
        /// </summary>
        /// <param name="disposing">false if pre-empted from a <c>finalizer</c></param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && InnerConnection != null)
            {
                InnerConnection.StateChange -= StateChangeHandler;
                InnerConnection.Dispose();
            }
            InnerConnection = null;
            DbProfiler = null;
            base.Dispose(disposing);
        }

        /// <summary>
        /// The state change handler.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="stateChangeEventArguments">The state change event arguments.</param>
        private void StateChangeHandler(object sender, StateChangeEventArgs stateChangeEventArguments)
        {
            OnStateChange(stateChangeEventArguments);
        }

        /// <summary>
        /// create a clone.
        /// </summary>
        /// <returns>The <see cref="ProfiledDbConnection"/>.</returns>
        public ProfiledDbConnection Clone()
        {
            var tail = InnerConnection as ICloneable;
            if (tail == null)
                throw new NotSupportedException("Underlying " + InnerConnection.GetType().Name + " is not cloneable");
            return new ProfiledDbConnection((DbConnection)tail.Clone(), DbProfiler);
        }

        /// <summary>
        /// create a clone.
        /// </summary>
        /// <returns>The <see cref="object"/>.</returns>
        object ICloneable.Clone()
        {
            return Clone();
        }
    };

    /// <summary>
    /// Categories of SQL statements.
    /// </summary>
    public enum SqlExecuteType : byte
    {
        /// <summary>
        /// Unknown type
        /// </summary>
        None = 0,

        /// <summary>
        /// DML statements that alter database state, e.g. INSERT, UPDATE
        /// </summary>
        NonQuery = 1,

        /// <summary>
        /// Statements that return a single record
        /// </summary>
        Scalar = 2,

        /// <summary>
        /// Statements that iterate over a result set
        /// </summary>
        Reader = 3
    }

    /// <summary>
    /// The profiled database transaction.
    /// </summary>
    public class ProfiledDbTransaction : DbTransaction
    {
        /// <summary>
        /// The connection.
        /// </summary>
        private ProfiledDbConnection _connection;

        /// <summary>
        /// The transaction.
        /// </summary>
        private DbTransaction _transaction;

        /// <summary>
        /// Initialises a new instance of the <see cref="ProfiledDbTransaction"/> class.
        /// </summary>
        /// <param name="transaction">The transaction.</param>
        /// <param name="connection">The connection.</param>
        public ProfiledDbTransaction(DbTransaction transaction, ProfiledDbConnection connection)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");
            if (connection == null) throw new ArgumentNullException("connection");
            _transaction = transaction;
            _connection = connection;
        }

        /// <summary>
        /// Gets the database connection.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get { return _connection; }
        }

        /// <summary>
        /// Gets the wrapped transaction.
        /// </summary>
        public DbTransaction WrappedTransaction
        {
            get { return _transaction; }
        }

        /// <summary>
        /// Gets the isolation level.
        /// </summary>
        public override IsolationLevel IsolationLevel
        {
            get { return _transaction.IsolationLevel; }
        }

        /// <summary>
        /// commit the transaction.
        /// </summary>
        public override void Commit()
        {
            _transaction.Commit();
        }

        /// <summary>
        /// rollback the transaction
        /// </summary>
        public override void Rollback()
        {
            _transaction.Rollback();
        }

        /// <summary>
        /// dispose the transaction and connection.
        /// </summary>
        /// <param name="disposing">false if being called from a <c>finalizer</c></param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && _transaction != null)
            {
                _transaction.Dispose();
            }
            _transaction = null;
            _connection = null;
            base.Dispose(disposing);
        }
    };

    /// <summary>
    /// This is a micro-cache; suitable when the number of terms is controllable (a few hundred, for example),
    /// and strictly append-only; you cannot change existing values. All key matches are on **REFERENCE**
    /// equality. The type is fully thread-safe.
    /// </summary>
    /// <typeparam name="TKey">the key type</typeparam>
    /// <typeparam name="TValue">the value type</typeparam>
    public class Link<TKey, TValue> where TKey : class
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="Link{TKey,TValue}"/> class.
        /// </summary>
        /// <param name="key">
        /// The key.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <param name="tail">
        /// The tail.
        /// </param>
        private Link(TKey key, TValue value, Link<TKey, TValue> tail)
        {
            Key = key;
            Value = value;
            Tail = tail;
        }

        /// <summary>
        /// Gets the key.
        /// </summary>
        public TKey Key { get; private set; }

        /// <summary>
        /// Gets the value.
        /// </summary>
        public TValue Value { get; private set; }

        /// <summary>
        /// Gets the tail.
        /// </summary>
        public Link<TKey, TValue> Tail { get; private set; }

        /// <summary>
        /// try and return a value from the cache based on the key.
        /// the default value is returned if no match is found.
        /// An exception is not thrown.
        /// </summary>
        /// <param name="link">The link.</param>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>return true if a value is located.</returns>
        public static bool TryGet(Link<TKey, TValue> link, TKey key, out TValue value)
        {
            while (link != null)
            {
                if ((object)key == (object)link.Key)
                {
                    value = link.Value;
                    return true;
                }

                link = link.Tail;
            }

            value = default(TValue);
            return false;
        }

        /// <summary>
        /// try and return a value from the cache based on the key.
        /// the default value is returned if no match is found.
        /// An exception is not thrown.
        /// </summary>
        /// <param name="head">The head.</param>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>return true if a value is located</returns>
        public static bool TryAdd(ref Link<TKey, TValue> head, TKey key, ref TValue value)
        {
            bool tryAgain;
            do
            {
                var snapshot = Interlocked.CompareExchange(ref head, null, null);
                TValue found;
                if (TryGet(snapshot, key, out found))
                {
                    // existing match; report the existing value instead
                    value = found;
                    return false;
                }

                var newNode = new Link<TKey, TValue>(key, value, snapshot);

                // did somebody move our cheese?
                tryAgain = Interlocked.CompareExchange(ref head, newNode, snapshot) != snapshot;
            } while (tryAgain);
            return true;
        }
    };

    /// <summary>
    /// The profiled database data reader.
    /// </summary>
    public class ProfiledDbDataReader : DbDataReader
    {
        /// <summary>
        /// The _reader.
        /// </summary>
        private readonly DbDataReader _reader;

        /// <summary>
        /// The _profiler.
        /// </summary>
        private readonly IDbProfiler _profiler;

        /// <summary>
        /// Initialises a new instance of the <see cref="ProfiledDbDataReader"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="connection">The connection.</param>
        /// <param name="profiler">The profiler.</param>
        public ProfiledDbDataReader(DbDataReader reader, DbConnection connection, IDbProfiler profiler)
        {
            _reader = reader;

            if (profiler != null)
            {
                _profiler = profiler;
            }
        }

        /// <summary>
        /// Gets the depth.
        /// </summary>
        public override int Depth
        {
            get { return _reader.Depth; }
        }

        /// <summary>
        /// Gets the field count.
        /// </summary>
        public override int FieldCount
        {
            get { return _reader.FieldCount; }
        }

        /// <summary>
        /// Gets a value indicating whether has rows.
        /// </summary>
        public override bool HasRows
        {
            get { return _reader.HasRows; }
        }

        /// <summary>
        /// Gets a value indicating whether is closed.
        /// </summary>
        public override bool IsClosed
        {
            get { return _reader.IsClosed; }
        }

        /// <summary>
        /// Gets the records affected.
        /// </summary>
        public override int RecordsAffected
        {
            get { return _reader.RecordsAffected; }
        }

        /// <summary>
        /// The 
        /// </summary>
        /// <param name="name">
        /// The name.
        /// </param>
        /// <returns>
        /// The <see cref="object"/>.
        /// </returns>
        public override object this[string name]
        {
            get { return _reader[name]; }
        }

        /// <summary>
        /// The 
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="object"/>.
        /// </returns>
        public override object this[int ordinal]
        {
            get { return _reader[ordinal]; }
        }

        /// <summary>
        /// The close.
        /// </summary>
        public override void Close()
        {
            // this can occur when we're not profiling, but we've inherited from ProfiledDbCommand and are returning a
            // an unwrapped reader from the base command
            if (_reader != null)
            {
                _reader.Close();
            }

            if (_profiler != null)
            {
                _profiler.ReaderFinish(this);
            }
        }

        /// <summary>
        /// The get boolean.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public override bool GetBoolean(int ordinal)
        {
            return _reader.GetBoolean(ordinal);
        }

        /// <summary>
        /// The get byte.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="byte"/>.
        /// </returns>
        public override byte GetByte(int ordinal)
        {
            return _reader.GetByte(ordinal);
        }

        /// <summary>
        /// The get bytes.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <param name="dataOffset">
        /// The data offset.
        /// </param>
        /// <param name="buffer">
        /// The buffer.
        /// </param>
        /// <param name="bufferOffset">
        /// The buffer offset.
        /// </param>
        /// <param name="length">
        /// The length.
        /// </param>
        /// <returns>
        /// The <see cref="long"/>.
        /// </returns>
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        /// <summary>
        /// The get char.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="char"/>.
        /// </returns>
        public override char GetChar(int ordinal)
        {
            return _reader.GetChar(ordinal);
        }

        /// <summary>
        /// The get chars.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <param name="dataOffset">
        /// The data offset.
        /// </param>
        /// <param name="buffer">
        /// The buffer.
        /// </param>
        /// <param name="bufferOffset">
        /// The buffer offset.
        /// </param>
        /// <param name="length">
        /// The length.
        /// </param>
        /// <returns>
        /// The <see cref="long"/>.
        /// </returns>
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        /// <summary>
        /// The get data type name.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public override string GetDataTypeName(int ordinal)
        {
            return _reader.GetDataTypeName(ordinal);
        }

        /// <summary>
        /// The get date time.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="DateTime"/>.
        /// </returns>
        public override DateTime GetDateTime(int ordinal)
        {
            return _reader.GetDateTime(ordinal);
        }

        /// <summary>
        /// The get decimal.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="decimal"/>.
        /// </returns>
        public override decimal GetDecimal(int ordinal)
        {
            return _reader.GetDecimal(ordinal);
        }

        /// <summary>
        /// The get double.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="double"/>.
        /// </returns>
        public override double GetDouble(int ordinal)
        {
            return _reader.GetDouble(ordinal);
        }

        /// <summary>
        /// The get enumerator.
        /// </summary>
        public override System.Collections.IEnumerator GetEnumerator()
        {
            return ((System.Collections.IEnumerable)_reader).GetEnumerator();
        }

        /// <summary>
        /// The get field type.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="Type"/>.
        /// </returns>
        public override Type GetFieldType(int ordinal)
        {
            return _reader.GetFieldType(ordinal);
        }

        /// <summary>
        /// The get float.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="float"/>.
        /// </returns>
        public override float GetFloat(int ordinal)
        {
            return _reader.GetFloat(ordinal);
        }

        /// <summary>
        /// get the GUID.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="Guid"/>.
        /// </returns>
        public override Guid GetGuid(int ordinal)
        {
            return _reader.GetGuid(ordinal);
        }

        /// <summary>
        /// The get integer.
        /// </summary>
        /// <param name="ordinal">The ordinal.</param>
        /// <returns>The <see cref="short"/>.</returns>
        public override short GetInt16(int ordinal)
        {
            return _reader.GetInt16(ordinal);
        }

        /// <summary>
        /// get a 32 bit integer
        /// </summary>
        /// <param name="ordinal">The ordinal.</param>
        /// <returns>
        /// The <see cref="int"/>.
        /// </returns>
        public override int GetInt32(int ordinal)
        {
            return _reader.GetInt32(ordinal);
        }

        /// <summary>
        /// get a 64 bit integer (long)
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="long"/>.
        /// </returns>
        public override long GetInt64(int ordinal)
        {
            return _reader.GetInt64(ordinal);
        }

        /// <summary>
        /// The get name.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public override string GetName(int ordinal)
        {
            return _reader.GetName(ordinal);
        }

        /// <summary>
        /// The get ordinal.
        /// </summary>
        /// <param name="name">
        /// The name.
        /// </param>
        /// <returns>
        /// The <see cref="int"/>.
        /// </returns>
        public override int GetOrdinal(string name)
        {
            return _reader.GetOrdinal(name);
        }

        /// <summary>
        /// The get schema table.
        /// </summary>
        /// <returns>
        /// The <see cref="DataTable"/>.
        /// </returns>
        public override DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }

        /// <summary>
        /// The get string.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public override string GetString(int ordinal)
        {
            return _reader.GetString(ordinal);
        }

        /// <summary>
        /// The get value.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="object"/>.
        /// </returns>
        public override object GetValue(int ordinal)
        {
            return _reader.GetValue(ordinal);
        }

        /// <summary>
        /// The get values.
        /// </summary>
        /// <param name="values">
        /// The values.
        /// </param>
        /// <returns>
        /// The <see cref="int"/>.
        /// </returns>
        public override int GetValues(object[] values)
        {
            return _reader.GetValues(values);
        }

        /// <summary>
        /// the database value null.
        /// </summary>
        /// <param name="ordinal">
        /// The ordinal.
        /// </param>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public override bool IsDBNull(int ordinal)
        {
            return _reader.IsDBNull(ordinal);
        }

        /// <summary>
        /// The next result.
        /// </summary>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public override bool NextResult()
        {
            return _reader.NextResult();
        }

        /// <summary>
        /// The read.
        /// </summary>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public override bool Read()
        {
            return _reader.Read();
        }
    };

    /// <summary>
    /// The profiled database command.
    /// </summary>
    [System.ComponentModel.DesignerCategory("")]
    public class ProfiledDbCommand : DbCommand, ICloneable
    {
        /// <summary>
        /// The bind by name cache.
        /// </summary>
        private static Link<Type, Action<IDbCommand, bool>> _bindByNameCache;

        /// <summary>
        /// The command.
        /// </summary>
        private DbCommand _command;

        /// <summary>
        /// The connection.
        /// </summary>
        private DbConnection _connection;

        /// <summary>
        /// The transaction.
        /// </summary>
        private DbTransaction _transaction;

        /// <summary>
        /// The profiler.
        /// </summary>
        private IDbProfiler _profiler;

        /// <summary>
        /// bind by name.
        /// </summary>
        private bool _bindByName;

        /// <summary>
        /// Gets or sets a value indicating whether or not to bind by name.
        /// If the underlying command supports BindByName, this sets/clears the underlying
        /// implementation accordingly. This is required to support OracleCommand from dapper-dot-net
        /// </summary>
        public bool BindByName
        {
            get { return _bindByName; }

            set
            {
                if (_bindByName != value)
                {
                    if (_command != null)
                    {
                        var inner = GetBindByName(_command.GetType());
                        if (inner != null) inner(_command, value);
                    }

                    _bindByName = value;
                }
            }
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="ProfiledDbCommand"/> class.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="connection">The connection.</param>
        /// <param name="profiler">The profiler.</param>
        public ProfiledDbCommand(DbCommand command, DbConnection connection, IDbProfiler profiler)
        {
            if (command == null) throw new ArgumentNullException("command");

            _command = command;
            _connection = connection;

            if (profiler != null)
            {
                _profiler = profiler;
            }
        }

        /// <summary>
        /// get the binding name.
        /// </summary>
        /// <param name="commandType">The command type.</param>
        /// <returns>The <see cref="Action"/>.</returns>
        private static Action<IDbCommand, bool> GetBindByName(Type commandType)
        {
            if (commandType == null) return null; // GIGO
            Action<IDbCommand, bool> action;
            if (Link<Type, Action<IDbCommand, bool>>.TryGet(_bindByNameCache, commandType, out action))
            {
                return action;
            }

            var prop = commandType.GetProperty("BindByName", BindingFlags.Public | BindingFlags.Instance);
            action = null;
            ParameterInfo[] indexers;
            MethodInfo setter;
            if (prop != null && prop.CanWrite && prop.PropertyType == typeof(bool)
                && ((indexers = prop.GetIndexParameters()) == null || indexers.Length == 0)
                && (setter = prop.GetSetMethod()) != null)
            {
                var method = new DynamicMethod(commandType.Name + "_BindByName", null,
                                               new[] { typeof(IDbCommand), typeof(bool) });
                var il = method.GetILGenerator();
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Castclass, commandType);
                il.Emit(OpCodes.Ldarg_1);
                il.EmitCall(OpCodes.Callvirt, setter, null);
                il.Emit(OpCodes.Ret);
                action = (Action<IDbCommand, bool>)method.CreateDelegate(typeof(Action<IDbCommand, bool>));
            }

            // cache it            
            Link<Type, Action<IDbCommand, bool>>.TryAdd(ref _bindByNameCache, commandType, ref action);
            return action;
        }

        /// <summary>
        /// Gets or sets the command text.
        /// </summary>
        public override string CommandText
        {
            get { return _command.CommandText; }
            set { _command.CommandText = value; }
        }

        /// <summary>
        /// Gets or sets the command timeout.
        /// </summary>
        public override int CommandTimeout
        {
            get { return _command.CommandTimeout; }
            set { _command.CommandTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the command type.
        /// </summary>
        public override CommandType CommandType
        {
            get { return _command.CommandType; }
            set { _command.CommandType = value; }
        }

        /// <summary>
        /// Gets or sets the database connection.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get { return _connection; }

            set
            {
                // TODO: we need a way to grab the IDbProfiler which may not be the same as the MiniProfiler, it could be wrapped
                // allow for command reuse, it is clear the connection is going to need to be reset
                if (DbProfiler.Current != null)
                {
                    _profiler = DbProfiler.Current;
                }

                _connection = value;
                var awesomeConn = value as ProfiledDbConnection;
                _command.Connection = awesomeConn == null ? value : awesomeConn.WrappedConnection;
            }
        }

        /// <summary>
        /// Gets the database parameter collection.
        /// </summary>
        protected override DbParameterCollection DbParameterCollection
        {
            get { return _command.Parameters; }
        }

        /// <summary>
        /// Gets or sets the database transaction.
        /// </summary>
        protected override DbTransaction DbTransaction
        {
            get { return _transaction; }

            set
            {
                _transaction = value;
                var awesomeTran = value as ProfiledDbTransaction;
                _command.Transaction = awesomeTran == null ? value : awesomeTran.WrappedTransaction;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the command is design time visible.
        /// </summary>
        public override bool DesignTimeVisible
        {
            get { return _command.DesignTimeVisible; }
            set { _command.DesignTimeVisible = value; }
        }

        /// <summary>
        /// Gets or sets the updated row source.
        /// </summary>
        public override UpdateRowSource UpdatedRowSource
        {
            get { return _command.UpdatedRowSource; }
            set { _command.UpdatedRowSource = value; }
        }

        /// <summary>
        /// The execute database data reader.
        /// </summary>
        /// <param name="behavior">The behaviour.</param>
        /// <returns>the resulting <see cref="DbDataReader"/>.</returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_profiler == null || !_profiler.IsActive)
            {
                return _command.ExecuteReader(behavior);
            }

            DbDataReader result = null;
            _profiler.ExecuteStart(this, SqlExecuteType.Reader);
            try
            {
                result = _command.ExecuteReader(behavior);
                result = new ProfiledDbDataReader(result, _connection, _profiler);
            }
            catch (Exception e)
            {
                _profiler.OnError(this, SqlExecuteType.Reader, e);
                throw;
            }
            finally
            {
                _profiler.ExecuteFinish(this, SqlExecuteType.Reader, result);
            }

            return result;
        }

        /// <summary>
        /// execute a non query.
        /// </summary>
        /// <returns>the number of affected records.</returns>
        public override int ExecuteNonQuery()
        {
            if (_profiler == null || !_profiler.IsActive)
            {
                return _command.ExecuteNonQuery();
            }

            int result;

            _profiler.ExecuteStart(this, SqlExecuteType.NonQuery);
            try
            {
                result = _command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                _profiler.OnError(this, SqlExecuteType.NonQuery, e);
                throw;
            }
            finally
            {
                _profiler.ExecuteFinish(this, SqlExecuteType.NonQuery, null);
            }

            return result;
        }

        /// <summary>
        /// execute the scalar.
        /// </summary>
        /// <returns>
        /// The <see cref="object"/>.
        /// </returns>
        public override object ExecuteScalar()
        {
            if (_profiler == null || !_profiler.IsActive)
            {
                return _command.ExecuteScalar();
            }

            object result;
            _profiler.ExecuteStart(this, SqlExecuteType.Scalar);
            try
            {
                result = _command.ExecuteScalar();
            }
            catch (Exception e)
            {
                _profiler.OnError(this, SqlExecuteType.Scalar, e);
                throw;
            }
            finally
            {
                _profiler.ExecuteFinish(this, SqlExecuteType.Scalar, null);
            }

            return result;
        }

        /// <summary>
        /// cancel the command.
        /// </summary>
        public override void Cancel()
        {
            _command.Cancel();
        }

        /// <summary>
        /// prepare the command.
        /// </summary>
        public override void Prepare()
        {
            _command.Prepare();
        }

        /// <summary>
        /// create a database parameter.
        /// </summary>
        /// <returns>The <see cref="DbParameter"/>.</returns>
        protected override DbParameter CreateDbParameter()
        {
            return _command.CreateParameter();
        }

        /// <summary>
        /// dispose the command.
        /// </summary>
        /// <param name="disposing">false if this is being disposed in a <c>finalizer</c>.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && _command != null)
            {
                _command.Dispose();
            }
            _command = null;
            base.Dispose(disposing);
        }

        /// <summary>
        /// Gets the internal command.
        /// </summary>
        public DbCommand InternalCommand
        {
            get { return _command; }
        }

        /// <summary>
        /// clone the command, entity framework expects this behaviour.
        /// </summary>
        /// <returns>The <see cref="ProfiledDbCommand"/>.</returns>
        public ProfiledDbCommand Clone()
        {
            // EF expects ICloneable
            var tail = _command as ICloneable;
            if (tail == null)
                throw new NotSupportedException("Underlying " + _command.GetType().Name + " is not cloneable");
            return new ProfiledDbCommand((DbCommand)tail.Clone(), _connection, _profiler);
        }

        /// <summary>
        /// The clone.
        /// </summary>
        /// <returns>
        /// The <see cref="object"/>.
        /// </returns>
        object ICloneable.Clone()
        {
            return Clone();
        }
    };
}
