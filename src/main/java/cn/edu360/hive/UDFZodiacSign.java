package cn.edu360.hive;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Description
public class UDFZodiacSign extends UDF {

    private SimpleDateFormat df;

    public UDFZodiacSign(){
        df = new SimpleDateFormat("MM-dd-yy");
    }

    public String evaluate(Date bday)
    {

        return this.evaluate(bday.getMonth() + 1, bday.getDay());
    }

    public String evaluate(String bday)
    {
        Date date = null;

        try {
            date = df.parse(bday);
        } catch (ParseException e) {
            return null;
        }
        return this.evaluate(date.getMonth() + 1, date.getDay());

    }

    public String evaluate(Integer month, Integer day)
    {
        if (month == 1)
        {
            if (day < 20)
            {
                return "Caprivorn";
            }else
                return "Aquarius";
        }
        if (month == 2)
        {
            return "aaa";
        }
        if (month == 3)
            return "bbb";

        if (month == 4)
            return 4+"aaa";
        else
            return "others";
    }
}
