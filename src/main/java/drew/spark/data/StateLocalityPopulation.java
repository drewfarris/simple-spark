package drew.spark.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class StateLocalityPopulation implements Serializable {

    static final Logger log = LoggerFactory.getLogger(StateLocalityPopulation.class);

    private String state;
    private String locality;
    private Long population;

    public StateLocalityPopulation(String state, String locality, Long population) {
        this.state = state;
        this.locality = locality;
        this.population = population;
    }

    public StateLocalityPopulation(String[] arr) {
        if (arr.length > 0) {
            state = arr[0];
        } else {
            state = "N/A";
        }

        if (arr.length > 1) {
            locality = arr[1];
        } else {
            locality = "N/A";
        }

        long tmpCount = 0L;
        if (arr.length > 2) {
            try {
                tmpCount = Long.parseLong(arr[2]);
            } catch (NumberFormatException nfe) {
                log.warn("Number Format Exception for input: " + Arrays.toString(arr));
            }
        }
        population = tmpCount;
    }

    public StateLocalityPopulation(String line) {
        this(line.split(", *"));
    }


    public StateLocalityPopulation() { }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public Long getPopulation() {
        return population;
    }

    public void setPopulation(Long population) {
        this.population = population;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StateLocalityPopulation)) return false;
        StateLocalityPopulation that = (StateLocalityPopulation) o;
        return Objects.equals(state, that.state) &&
                Objects.equals(locality, that.locality) &&
                Objects.equals(population, that.population);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, locality, population);
    }

    public static Iterator<Tuple2<StateLocalityPopulation, Void>> flatMap(String[] arr) {
        List<Tuple2<StateLocalityPopulation, Void>> result = new ArrayList<>();
        if (arr.length >= 3) {
            result.add(new Tuple2<>(new StateLocalityPopulation(arr), null));
        }
        return result.iterator();
    }

    public String toString() {
        return String.join(", ", state, locality, String.valueOf(population));
    }
}
