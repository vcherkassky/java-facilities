package jf.jdbc;

import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class SqlFacility {
    private static final Logger log = LoggerFactory.getLogger(SqlFacility.class);

    private final DataSource dataSource;

    public SqlFacility(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void executeSql(String sql) {
        WithConnection withConnection = new WithConnection();
        withConnection.addExecutable(new ExecuteSql(withConnection, sql));
        withConnection.execute();
    }

    public <T> T executeQuery(String sql, ResultTransformer<T> transformer) {
        WithConnection withConnection = new WithConnection();
        ExecuteQuery<T> query = new ExecuteQuery<T>(withConnection, sql, transformer);
        withConnection.addExecutable(query);
        withConnection.execute();

        return query.getResult();
    }

    public <T> Future<T> executeQueryAsync(String sql, ResultTransformer<T> transformer, ExecutorService executorService) {
        final WithConnection withConnection = new WithConnection();
        final ExecuteQuery<T> query = new ExecuteQuery<T>(withConnection, sql, transformer);
        withConnection.addExecutable(query);

        return executorService.submit(new Callable<T>() {
            @Override
            public T call() throws Exception {
                withConnection.execute();
                return query.getResult();
            }
        });
    }

    private interface Executable {
        void execute();
        boolean failover(RuntimeException exception);
        void cleanup();
    }

    private interface ExecutableWithResult<R> extends Executable {
        R getResult();
    }

    private abstract class ExecutableContext<C> implements Executable {
        private final List<Executable> executables = new ArrayList<Executable>();

        protected abstract C getContext();

        public void addExecutable(Executable executable) {
            checkNotNull(executable);
            executables.add(executable);
        }

        public void execute() {
            try {
                for (Executable executable : executables) {
                    try {
                        executable.execute();
                    } catch (RuntimeException e) {
                        if (executable.failover(e)) {
                            log.info("Failover succeeded, retrying original operation");
                            try {
                                executable.execute();
                            } catch (RuntimeException e1) {
                                log.error("Operation failed again after successful failover", e1);
                            }
                        }
                        throw e;
                    } finally {
                        executable.cleanup();
                    }
                }
            } finally {
                cleanup();
            }
        }

        @Override
        public boolean failover(RuntimeException exception) {
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +
                    ".Ctx execute [ " + Joiner.on(" -> cleanup, ").join(executables) + " -> cleanup ] -> cleanup";
        }
    }

    private class WithConnection extends ExecutableContext<Connection> {
        private Connection connection;

        @Override
        protected Connection getContext() {
            if (!validConnection()) {
                try {
                    connection = dataSource.getConnection();
                } catch (SQLException e) {
                    log.error("Could not get connection from datasource " + dataSource, e);
                    throw Throwables.propagate(e);
                }
            }
            return connection;
        }

        private boolean validConnection() {
            try {
                return connection != null && !connection.isClosed();
            } catch (SQLException e) {
                log.error("Could not check if connection is closed", e);
                return false;
            }
        }

        @Override
        public void cleanup() {
            closeConnection(connection);
        }
    }

    private class SingleTransaction extends ExecutableContext<Connection> {
        private final WithConnection withConnection;

        private SingleTransaction(WithConnection withConnection) {
            this.withConnection = withConnection;
        }

        @Override
        protected Connection getContext() {
            Connection connection = withConnection.getContext();
            try {
                // starting a manual transaction
                if (connection.getAutoCommit()) {
                    connection.setAutoCommit(false);
                }
            } catch (SQLException e) {
                logDbError("Could not start a manual transaction for datasource " + dataSource, e);
                throw Throwables.propagate(e);
            }
            return connection;
        }

        @Override
        public boolean failover(RuntimeException exception) {
            Connection connection = withConnection.getContext();
            try {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                }
            } catch (SQLException e) {
                logDbError("Could not rollback transaction for datasource " + dataSource, e);
            }
            return false;
        }

        @Override
        public void cleanup() {
            Connection connection = withConnection.getContext();
            try {
                if (!connection.getAutoCommit()) {
                    connection.commit();
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                logDbError("Could not commit a manual transaction for datasource " + dataSource, e);
                throw Throwables.propagate(e);
            }
        }
    }

    private abstract class ContextAwareExecutable<T> implements Executable {
        protected final ExecutableContext<T> executableContext;

        protected ContextAwareExecutable(ExecutableContext<T> executableContext) {
            this.executableContext = executableContext;
        }

        public final void execute() {
            T context = executableContext.getContext();
            execute(context);
        }

        protected abstract void execute(T context);

        @Override
        public final boolean failover(RuntimeException exception) {
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(getContext -> execute -x failover -> execute)";
        }
    }

    private class ExecuteSql extends ContextAwareExecutable<Connection> {

        private final String sql;
        private Statement statement;

        private ExecuteSql(ExecutableContext<Connection> context, String sql) {
            super(context);
            this.sql = sql;
        }

        @Override
        protected void execute(Connection connection) {
            try {
                try {
                    statement = connection.createStatement();
                } catch (SQLException e) {
                    log.error("Could not create statement", e);
                    throw Throwables.propagate(e);
                }
                try {
                    statement.execute(sql);
                } catch (SQLException e) {
                    logDbError(format("Could not execute sql \"%s\" using dataSource %s", cutIfTooLong(sql), dataSource), e);
                    throw Throwables.propagate(e);
                }
            } finally {
                closeStatement(statement);
            }
        }

        @Override
        public void cleanup() {
            closeStatement(statement);
        }
    }

    private class ExecuteBatchSql extends ContextAwareExecutable<Connection> {

        private final List<String> queries;
        private Statement statement;

        private ExecuteBatchSql(ExecutableContext<Connection> executableContext, List<String> queries) {
            super(executableContext);
            this.queries = queries;
        }

        @Override
        protected void execute(Connection connection) {
            try {
                try {
                    statement = connection.createStatement();
                } catch (SQLException e) {
                    log.error("Could not create statement", e);
                    throw Throwables.propagate(e);
                }
                for (String sql : queries) {
                    try {
                        statement.execute(sql);
                    } catch (SQLException e) {
                        logDbError(format("Could not execute sql \"%s\" using dataSource %s", cutIfTooLong(sql), dataSource), e);
                        throw Throwables.propagate(e);
                    }
                }
            } finally {
                closeStatement(statement);
            }
        }

        @Override
        public void cleanup() {
            closeStatement(statement);
        }
    }

    private class ExecutePrepared<T> extends ContextAwareExecutable<Connection> {
        private final String sql;
        private final T params;
        private final StatementBinder<T> binder;

        private PreparedStatement statement;

        private ExecutePrepared(ExecutableContext<Connection> context, String sql, T params, StatementBinder<T> binder) {
            super(context);
            this.sql = sql;
            this.params = params;
            this.binder = binder;
        }

        @Override
        protected void execute(Connection context) {
            try {
                statement = context.prepareStatement(sql);
            } catch (SQLException e) {
                log.error("Could not create statement", e);
                throw Throwables.propagate(e);
            }
            try {
                binder.bindParams(params, statement);
                statement.executeBatch();
            } catch (SQLException e) {
                logDbError(format("Could not execute query with params: %s, %nsql: %s using dataSource %s", params,
                        cutIfTooLong(sql), dataSource), e);
                throw Throwables.propagate(e);
            }
            closeStatement(statement);
        }

        @Override
        public void cleanup() {
            closeStatement(statement);
        }
    }

    public interface ResultTransformer<T> {
        T transform(ResultSet resultSet) throws SQLException;
    }

    public static abstract class SingleRowTransformer<T> implements ResultTransformer<T> {

        protected abstract T transformRow(ResultSet resultSet) throws SQLException;

        public T transform(ResultSet resultSet) throws SQLException {
            resultSet.next();
            return transformRow(resultSet);
        }
    }

    public interface StatementBinder<T> {
        void bindParams(T params, PreparedStatement statement) throws SQLException;
    }

    public static abstract class IterableBinder<T> implements StatementBinder<Iterable<T>> {

        protected abstract void bindParam(T param, PreparedStatement statement) throws SQLException;

        @Override
        public final void bindParams(Iterable<T> params, PreparedStatement statement) throws SQLException {
            for (T param : params) {
                bindParam(param, statement);
                try {
                    statement.addBatch();
                } catch (SQLException e) {
                    log.error(format("Could not add param \"%s\" to batch", param));
                    throw e;
                }
            }
        }
    }

    private class ExecuteQueryRaw extends ContextAwareExecutable<Connection> implements ExecutableWithResult<ResultSet> {
        private final String sql;

        Statement statement;
        ResultSet resultSet;

        private ExecuteQueryRaw(ExecutableContext<Connection> context, String sql) {
            super(context);
            this.sql = sql;
        }

        @Override
        protected void execute(Connection context) {
            try {
                statement = context.createStatement();
            } catch (SQLException e) {
                log.error("Could not create statement", e);
                throw Throwables.propagate(e);
            }
            try {
                Timer.Context executeQryTime = time("DB.executeQuery");

                resultSet = statement.executeQuery(sql);

                executeQryTime.close();
            } catch (SQLException e) {
                log.error(format("Could not execute query \"%s\" using dataSource %s", cutIfTooLong(sql), dataSource), e);
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void cleanup() {
            closeResultSet(resultSet);
            closeStatement(statement);
        }

        @Override
        public ResultSet getResult() {
            return resultSet;
        }
    }

    private class ExecuteQuery<T> extends ContextAwareExecutable<Connection> implements ExecutableWithResult<T> {
        private final ExecuteQueryRaw raw;
        private final ResultTransformer<T> transformer;
        private T result;

        private ExecuteQuery(ExecutableContext<Connection> context, String sql, ResultTransformer<T> transformer) {
            super(context);
            raw = new ExecuteQueryRaw(context, sql);
            this.transformer = transformer;
        }

        @Override
        protected void execute(Connection context) {
            raw.execute();
            try {
                result = transformer.transform(raw.getResult());
            } catch (SQLException e) {
                log.error(format("Could not execute query \"%s\" using dataSource %s", cutIfTooLong(raw.sql), dataSource), e);
                throw Throwables.propagate(e);
            } catch (RuntimeException e) {
                log.error(format("Could not execute query \"%s\" using dataSource %s", cutIfTooLong(raw.sql), dataSource), e);
                throw e;
            }
            closeResultSet(raw.resultSet);
            closeStatement(raw.statement);
        }

        @Override
        public void cleanup() {
            closeResultSet(raw.resultSet);
            closeStatement(raw.statement);
        }

        @Override
        public T getResult() {
            return result;
        }
    }

    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("Could not close connection", e);
            }
        }
    }

    public static void closeStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (Exception e) {
                log.error("Could not close statement", e);
            }
        }
    }

    public static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception e) {
                log.error("Could not close resultSet", e);
            }
        }
    }

    public static String cutIfTooLong(String sql) {
        final int lengthLimit = 2048;
        if (sql.length() > lengthLimit) {
            String sqlCut = sql.substring(0, lengthLimit);
            return format("%s%n  ...%n(only %d characters are shown)", sqlCut, lengthLimit);
        }
        return sql;
    }

    public Constructor constructor() {
        return new Constructor();
    }

    public class Constructor {
        private final WithConnection rootContext;
        private ExecutableContext<Connection> currentContext;

        private Constructor() {
            rootContext = new WithConnection();
            currentContext = rootContext;
        }

        private Constructor(WithConnection rootContext, ExecutableContext<Connection> currentContext) {
            this.rootContext = rootContext;
            this.currentContext = currentContext;
        }

        public Constructor singleTransaction() {
            if (! (currentContext instanceof WithConnection)) {
                throw new IllegalStateException("Nested transactions are not supported yet");
            }
            SingleTransaction transaction = new SingleTransaction(rootContext);
            rootContext.addExecutable(transaction);
            return new Constructor(rootContext, transaction);
        }

        public Constructor addSql(String sql) {
            currentContext.addExecutable(new ExecuteSql(currentContext, sql));
            return this;
        }

        public Constructor addBatchSql(List<String> batchSql) {
            currentContext.addExecutable(new ExecuteBatchSql(currentContext, batchSql));
            return this;
        }

        public<T> ForResult<T> addQuery(String sql, ResultTransformer<T> transformer) {
            ExecuteQuery<T> query = new ExecuteQuery<T>(currentContext, sql, transformer);
            currentContext.addExecutable(query);
            return new ForResult<T>(this, query);
        }

        public<T> Constructor addSqlWithParams(String sql, T params, StatementBinder<T> binder) {
            ExecutePrepared<T> prepared = new ExecutePrepared<T>(currentContext, sql, params, binder);
            currentContext.addExecutable(prepared);
            return this;
        }

        public void execute() {
            rootContext.execute();
        }
    }

    public class ForResult<T> extends Constructor {
        private final ExecuteQuery<T> query;

        public ForResult(Constructor constructor, ExecuteQuery<T> query) {
            super(constructor.rootContext, constructor.currentContext);
            this.query = query;
        }

        public T result() {
            return query.getResult();
        }
    }

    private static void logDbError(String logMessage, SQLException sqlException) {
        log.error(logMessage, sqlException);
        if (sqlException.getMessage().contains("getNextException")) {
            log.error("The cause is", sqlException.getNextException());
        }
    }
}
