package org.apache.drill.exec.expr.fn.impl;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import java.text.ParseException;
import java.util.Date;

public class DateTypeAutoFunctions {
  @SuppressWarnings("unused")
  @FunctionTemplate(names = { "date_add",
    "add" }, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class DateTimeAddAutoFunction implements DrillSimpleFunc {
    @Param
    VarCharHolder left;
    @Param
    VarCharHolder right;
    @Output
    TimeStampHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      String defaultDateFormat = "yyyy/mm/dd";
      FastDateFormat defaultDateFormatter = org.apache.commons.lang3.time.FastDateFormat.getInstance(defaultDateFormat);
      String leftStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(left);
      String rightStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(right);

      if (org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.isReadableAsDate(left.buffer,
        left.start, left.end)) {
        try {
          Date inputDate = defaultDateFormatter.parse(leftStr);
          int inputInt = Integer.parseInt(rightStr);
          DateHolder myDateHolder = new DateHolder();
          myDateHolder.value = inputDate.getTime();
          System.out.println("dateholder: " + myDateHolder.value);
          TimeHolder myTimeHolder = new TimeHolder();
          myTimeHolder.value = inputInt;
          System.out.println("timeholder: " + myTimeHolder.value);
          out.value = myDateHolder.value + myTimeHolder.value;
          System.out.println("out: " + out.value);
        } catch(ParseException e) {
          System.out.println("unable to parse-parse");
        } catch(NumberFormatException e) {
          System.out.println("nfe");
        }
      }

      // check if isTimestamp = true
      //

      //out.value = left.value + right.value;
    }
  }
}
