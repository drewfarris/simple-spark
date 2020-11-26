package drew.spark;

import drew.spark.sort.SecondarySort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class Driver {

    public static final Logger log = LoggerFactory.getLogger(Driver.class);

    public static void main(String[] args) {
        String option = args[0];
        String[] remaining = new String[args.length-1];
        System.arraycopy(args, 1, remaining, 0, args.length - 1);

        switch (option) {
            case "simple":
                break;
            case "spark":
                break;
            case "sort":
                SecondarySort ss = new SecondarySort(remaining);
                ss.execute();
                break;
            default:
                Executable e = instantiateClass(option, remaining);
                if (e != null) {
                    e.execute();
                }
                break;

        }
    }

    public static Executable instantiateClass(String option, String[] args) {
        try {
            ClassLoader cl = Driver.class.getClassLoader();
            Class<?> clazz =cl.loadClass(option);
            if (Executable.class.isAssignableFrom(clazz)) {
                Constructor<?> ctor = clazz.getConstructor(String[].class);
                return (Executable) ctor.newInstance(new Object[]{args});
            }
        }
        catch (ClassNotFoundException | NoSuchMethodException e) {
            log.error("Could not find class or String[] constructor for " + option, e);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            log.error("Could not instantiate " + option, e);
        }

        return null;
    }

    public static interface Executable {
        void execute();
    }
}
