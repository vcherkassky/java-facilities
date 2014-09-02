package jf.jdbc;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

@RunWith(JUnit4.class)
public class SimpleQueryTest {

    private static String DB_URL = "jdbc:hsqldb:mem:test";

    private DataSource dataSource;

    @Before
    public void setUpDataSource() {
        JDBCDataSource jdbcDataSource = new JDBCDataSource();
        jdbcDataSource.setUrl(DB_URL);
        dataSource = jdbcDataSource;
    }

    @Test
    public void select_works() {
        SqlFacility facility = new SqlFacility(dataSource);
        SqlFacility.Constructor constructor = facility.constructor()
                .addSql("CREATE TABLE user(id INTEGER primary key, name VARCHAR(255))")
                .addSqlWithParams("INSERT INTO user VALUES(?, ?)", 1, "Bill")
                .addSqlWithParams("INSERT INTO user VALUES(?, ?)", 2, "Jim");

        SqlFacility.ForResult<List<User>> users = constructor.addQuery("SELECT id, name FROM user",
                new SqlFacility.ResultTransformer<List<User>>() {
                    @Override
                    public List<User> transform(ResultSet rs) throws SQLException {
                        List<User> result = new ArrayList<User>();
                        while (rs.next()) {
                            result.add(new User(rs.getInt(1), rs.getString(2)));
                        }
                        return result;
                    }
                });

        constructor.execute();

        List<User> actual = users.result();
        User[] expected = { new User(1, "Bill"), new User(2, "Jim") };
        assertArrayEquals(expected, actual.toArray());
    }

    private static class User {
        private final int id;
        private final String name;

        private User(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            User user = (User) o;

            if (id != user.id) return false;
            if (name != null ? !name.equals(user.name) : user.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }
}
