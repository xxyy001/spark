package spark.javaVersion.sql;


import java.io.Serializable;

public class Student implements Serializable{
    private int id;
    private int age;
    private String name;

    public void setId(int id) {
        this.id = id;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {

        return id;
    }

    public int getAge() {
        return age;
    }

    public String getName() {
        return name;
    }
}


