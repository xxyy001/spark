package spark.javaVersion.core;

import scala.Serializable;
import scala.math.Ordered;

import java.util.Objects;

public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {
    private int first;
    private int second;

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecondarySortKey that = (SecondarySortKey) o;
        return first == that.first &&
                second == that.second;
    }

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int hashCode() {

        return Objects.hash(first, second);
    }


    public int compare(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0){
            return this.first - that.getFirst();
        }else {
            return this.second - that.getSecond();
        }
    }

    public boolean $less(SecondarySortKey that) {
        if(this.first < that.getFirst()){
            return true;
        }else if(this.first == that.getFirst() && that.second < that.getSecond()){
            return true;
        }
        return false;
    }

    public boolean $greater(SecondarySortKey that) {
        if(this.first > that.getFirst()){
            return true;
        }else if(this.first == that.getFirst() && that.second >that.getSecond()){
            return true;
        }
        return false;
    }

    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that)){
            return true;
        }else if (this.first == that.getFirst() && that.second == that.getSecond()){
            return true;
        }
        return false;
    }

    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)){
            return true;
        }else if (this.first == that.getFirst() && that.second == that.getSecond()){
            return true;
        }
        return false;
    }

    public int compareTo(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0){
            return this.first - that.getFirst();
        }else {
            return this.second - that.getSecond();
        }
    }

}
