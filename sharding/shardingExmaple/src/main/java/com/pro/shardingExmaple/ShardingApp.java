package com.pro.shardingExmaple;


import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ShardingApp {
    public static void main(String[] args) {


        var shards = 3;
        List<JdbcTemplate> jdbcTemplates = getjdbcTemplate(shards);
        TodoRepository repository= new TodoRepository(jdbcTemplates,new HashShardResolver()) ;
        repository.insert();
    }
    public  static  List<JdbcTemplate> getjdbcTemplate(int shard){
//  "jdbc:postgresql://localhost:555%d/postgres".formatted(i),
        return IntStream.range(0,shard)
                .mapToObj(x->{
                     var datasource = new DriverManagerDataSource() ;
                     datasource.setPassword("postgres");
                     datasource.setUsername("postgres");
                     datasource.setUrl("jdbc:postgresql://localhost:555%d/postgres".formatted(x));
                    return  new JdbcTemplate(datasource);
                }).collect(Collectors.toList());
    }
}

interface ShardResolver {
    int getShardId(int id,int numberOfShard);
}

class HashShardResolver implements ShardResolver {

    @Override
    public int getShardId(int id, int numberOfShard) {
        return  Objects.hash(id)%numberOfShard;
    }
}
record Todo(int id , String name, String description){}
class TodoRepository {


    private final List<JdbcTemplate> jdbcTemplates;
    private final ShardResolver shardResolver;

    public TodoRepository(List<JdbcTemplate> jdbcTemplates, ShardResolver resolver) {

        this.jdbcTemplates = jdbcTemplates;
        this.shardResolver = resolver;
    }
    public void insert(){
       // insert data of 1milion
       for(int i=0;i<1000_000;i++) {
           int shardid =this.shardResolver.getShardId(i,jdbcTemplates.size());
           var jdbc=jdbcTemplates.get(shardid);
           var todo= new Todo(i,"todo-"+i,"todo-desc-"+i);
           jdbc.update("insert into todos(id, name, description) values (?,?,?)",todo.id(),todo.name(),todo.description());
           System.out.println("  todo "+i +" : inserted at postgres-shard-"+shardid);

       }
    }

}
