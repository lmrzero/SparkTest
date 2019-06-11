package cn.edu360.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * created by LMR on 2019/5/24
 */
public class GenericUDFNvl extends GenericUDF {

    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[] argumentOIs;
    /**
     * Initialize this GenericUDF. This will be called once and only once per
     * GenericUDF instance.
     *
     * @param arguments The ObjectInspector for the arguments
     * @return The ObjectInspector for the return value
     * @throws UDFArgumentException Thrown when arguments have wrong types, wrong length, etc.
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments;
        if (arguments.length != 2)
        {
            throw new UDFArgumentLengthException("");
        }

        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        if (!(returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1]))){

            throw new UDFArgumentTypeException(2, "");
        }
        return returnOIResolver.get();
    }

    /**
     * Evaluate the GenericUDF with the arguments.
     *
     * @param arguments The arguments as DeferedObject, use DeferedObject.get() to get the
     *                  actual argument Object. The Objects can be inspected by the
     *                  ObjectInspectors passed in the initialize call.
     * @return The
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        Object reVal = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);

        if (reVal == null)
        {
            reVal = returnOIResolver.convertIfNecessary(arguments[1].get(), argumentOIs[1]);
        }
        return reVal;
    }

    /**
     * Get the String to be displayed in explain.
     *
     * @param children
     */
    @Override
    public String getDisplayString(String[] children) {

        StringBuffer sb = new StringBuffer();
        sb.append("if");
        sb.append(children[0]);
        sb.append(" is null ");
        sb.append("returns");
        sb.append(children[1]);


      //  GenericUDAFAverage
        return sb.toString();
    }
}
