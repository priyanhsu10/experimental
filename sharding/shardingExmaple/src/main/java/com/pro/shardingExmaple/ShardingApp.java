package com.pro.shardingExmaple;


import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ShardingApp {
    public static void main(String[] args) {


        var shards = 3;
        List<JdbcTemplate> jdbcTemplates = getjdbcTemplate(shards);
        TodoRepository repository = new TodoRepository(jdbcTemplates, new HashShardResolver());
//        repository.insert();
//
//        try{
//           Thread.sleep(10000);
//        }catch (Exception ex){}
//        System.out.println(repository.getOf(509));
//        System.out.println(repository.getOf(102));
//        System.out.println(repository.getOf(25));
        System.out.println(repository.AllOfNameLike("aa"));
    }

    public static List<JdbcTemplate> getjdbcTemplate(int shard) {
//  "jdbc:postgresql://localhost:555%d/postgres".formatted(i),
        return IntStream.range(0, shard)
                .mapToObj(x -> {
                    var datasource = new DriverManagerDataSource();
                    datasource.setPassword("postgres");
                    datasource.setUsername("postgres");
                    datasource.setUrl("jdbc:postgresql://localhost:555%d/postgres".formatted(x));
                    return new JdbcTemplate(datasource);
                }).collect(Collectors.toList());
    }
}

interface ShardResolver {
    int getShardId(int id, int numberOfShard);
}

class HashShardResolver implements ShardResolver {

    @Override
    public int getShardId(int id, int numberOfShard) {
        return Objects.hash(id) % numberOfShard;
    }
}

record Todo(int id, String name, String description) {
}

class TodoRepository {


    private final List<JdbcTemplate> jdbcTemplates;
    private final ShardResolver shardResolver;
    private final static Random RANDOM = new Random();
    private static final char[] CHARS = "abcdfghijklmnopqrstwvuxyz0123456789".toCharArray();

    public TodoRepository(List<JdbcTemplate> jdbcTemplates, ShardResolver resolver) {

        this.jdbcTemplates = jdbcTemplates;
        this.shardResolver = resolver;
    }

    private String getRandomName() {
        var chars = new char[RANDOM.nextInt(50)];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = CHARS[RANDOM.nextInt(chars.length)];
        }
        return new String(chars);
    }

    public void insert() {
        // insert data of 1million
        var executor = Executors.newFixedThreadPool(250);
        IntStream.range(1, 1000)

                .forEach(i -> {

                    executor.submit(() -> {
                        int shardid = this.shardResolver.getShardId(i, jdbcTemplates.size());
                        var jdbc = jdbcTemplates.get(shardid);
                        var todo = new Todo(i, getRandomName(), "todo-desc-" + i);
                        jdbc.update("insert into todos(id, name, description) values (?,?,?)", todo.id(), todo.name(), todo.description());
                        System.out.println("  todo " + todo.id() + " : inserted at postgres-shard-" + shardid);
                    });
                });
        executor.shutdown();


    }

    public Optional<Todo> getOf(int id) {
        var shard = this.shardResolver.getShardId(id, jdbcTemplates.size());
        var template = jdbcTemplates.get(shard);
        var result = template.query("select * from todos where id= ?", (resultSet, rownum) -> new Todo(id, resultSet.getString("name"), resultSet.getString("description")), id);
        return result.isEmpty() ? Optional.empty() : Optional.of(result.get(0));
    }

    public List<Todo> AllOfNameLike(String str) {

        return jdbcTemplates.parallelStream()
                        .map(t -> t.query("select * from todos where name ilike ?",
                                (rs, i) -> new Todo(rs.getInt("id"),
                                        rs.getString("name"),
                                        rs.getString("description")),"%"+str+"%")
                        )
                        .flatMap(x -> x.stream())
                        .toList();

    }
}
