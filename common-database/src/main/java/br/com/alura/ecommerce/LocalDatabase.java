package br.com.alura.ecommerce;

import java.sql.*;

public class LocalDatabase {
    private final Connection connection;

    public LocalDatabase(String databaseName) throws SQLException {
        String url = "jdbc:sqlite:target/" + databaseName + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public boolean update(String statement, String... params) throws SQLException {
        return prepare(statement, params).execute();
    }

    public ResultSet query(String query, String ... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }

    private PreparedStatement prepare(String query, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(query);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i+1, params[i]);
        }
        return preparedStatement;
    }
}
