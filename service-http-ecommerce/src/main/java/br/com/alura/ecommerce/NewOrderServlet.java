package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
        try {
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = req.getParameter("uuid");

            var order = new Order(orderId, email, amount);

            try (var datatabase = new OrdersDatabase()) {
                if(datatabase.saveNew(order)) {
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

                    System.out.println("New order sent successfully.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent");
                } else {
                    System.out.println("Old order received.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received.");
                }
            }

        } catch (ExecutionException | InterruptedException | IOException | SQLException e) {
            throw new ServletException(e);
        }
    }
}
