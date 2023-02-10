package fop.w10join;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Database {

    private static Path baseDataDirectory = Paths.get("data");

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }

    public static Stream<Customer> processInputFileCustomer() throws IOException {
        Path path = Paths.get((baseDataDirectory) + "/customer.tbl");
        try {


            return Files.lines(path)
                    .map(line -> {
                        String[] custy = line.split("\\|");

                        return new Customer(Integer.parseInt(custy[0]), custy[2].toCharArray(),
                                Integer.parseInt(custy[3]), custy[4].toCharArray(),
                                Float.parseFloat(custy[5]), custy[6], custy[7].toCharArray());


                    });
        } catch (IOException | NumberFormatException | DateTimeParseException e) {
            e.printStackTrace();
            return Stream.empty();
        }

    }

    public static Stream<LineItem> processInputFileLineItem() throws IOException {
        Path path = Paths.get((baseDataDirectory) + "/lineitem.tbl");
        try {

            return Files.lines(path).map(line -> {
                String[] liney = line.split("\\|");


                return new LineItem(Integer.parseInt(liney[0]), Integer.parseInt(liney[1]), Integer.parseInt(liney[2]),
                        Integer.parseInt(liney[3]), Integer.parseInt(liney[4]) * 100, Float.parseFloat(liney[5]),
                        Float.parseFloat(liney[6]), Float.parseFloat(liney[7]), liney[8].charAt(0),
                        liney[9].charAt(0), LocalDate.parse(liney[10]), LocalDate.parse(liney[11]), LocalDate.parse(liney[12]),
                        liney[13].toCharArray(), liney[14].toCharArray(), liney[15].toCharArray());
            });
        } catch (IOException | NumberFormatException | DateTimeParseException e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }


    public static Stream<Order> processInputFileOrders() {
        Path path = Paths.get((baseDataDirectory) + "/orders.tbl");
        try {

            return Files.lines(path).map(line -> {
                String[] ordery = line.split("\\|");

                return new Order(Integer.parseInt(ordery[0]), Integer.parseInt(ordery[1]), ordery[2].charAt(0),
                        Float.parseFloat(ordery[3]), LocalDate.parse(ordery[4]), ordery[5].toCharArray(),
                        ordery[6].toCharArray(), Integer.parseInt(ordery[7]), ordery[8].toCharArray());
            });


        } catch (IOException | NumberFormatException | DateTimeParseException e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    public long getAverageQuantityPerMarketSegment(String marketSegment) {
        try {
            Stream<Customer> customers = processInputFileCustomer();
            Stream<Order> orders = processInputFileOrders();
            Stream<LineItem> lineItems = processInputFileLineItem();

            final long[] quantity = {0};
            final long[] num = {0};


            Map<Integer, String> custy = customers
                    .collect(Collectors
                            .toMap(x -> x.custKey, y -> y.mktsegment));


            Map<Integer, String> ordy = orders
                    .collect(Collectors
                            .toMap(x -> x.orderKey, y -> custy.get(y.custKey)));


            lineItems.forEach(lineItem -> {
                if ( ordy.get(lineItem.orderKey).equals(marketSegment)) {
                    num[0]++;
                    quantity[0] += lineItem.quantity;
                }
            });



            return quantity[0] / num[0];


        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public Database() {}

    public static void main(String[] args) throws IOException {

        processInputFileOrders().forEach(x -> System.out.println(x));
        processInputFileLineItem().forEach(x -> System.out.println(x));
        processInputFileCustomer().forEach(x -> System.out.println(x));


        Database db = new Database();
        db.getAverageQuantityPerMarketSegment("MACHINERY"); // 2539
        db.getAverageQuantityPerMarketSegment("AUTOMOBILE"); // 2578
        db.getAverageQuantityPerMarketSegment("FURNITURE");  // 2577
        db.getAverageQuantityPerMarketSegment("HOUSEHOLD");  // 2486
        db.getAverageQuantityPerMarketSegment("BUILDING");   // 2497
    }
}
