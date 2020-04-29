package org.apidb.apicomplexa.wsfplugin.spanlogic;

import org.gusdb.wsf.plugin.PluginUserException;

public class SpanCompositionTestCase {

  public int id;
  public String description;
  public String[] inputA;
  public String[] inputB;

  public String beginA;
  public String beginDirectionA;
  public String beginOffsetA;
  public String endA;
  public String endDirectionA;
  public String endOffsetA;

  public String beginB;
  public String beginDirectionB;
  public String beginOffsetB;
  public String endB;
  public String endDirectionB;
  public String endOffsetB;

  public String operator;
  public String outputFrom = "a";
  public String strand = "both_strands";

  public String[] expectedOutput;
  public String[] actualOutput;
  public boolean success;

  public SpanCompositionTestCase(String description)
  throws PluginUserException {
    this.description = description;

    // parse the input
    String[] parts = description.split("\t+");
    this.inputA = parts[0].trim().split("\\s*,\\s*");
    this.inputB = parts[1].trim().split(",");

    this.beginA = parts[2].trim();
    this.beginDirectionA = parts[3].trim();
    this.beginOffsetA = Integer.valueOf(parts[4].trim()).toString();

    this.endA = parts[5].trim();
    this.endDirectionA = parts[6].trim();
    this.endOffsetA = Integer.valueOf(parts[7].trim()).toString();

    this.operator = parts[8].trim();
    if (operator.equals("contains"))
      operator = SpanCompositionPlugin.PARAM_VALUE_A_CONTAIN_B;
    else if (operator.equals("contained"))
      operator = SpanCompositionPlugin.PARAM_VALUE_B_CONTAIN_A;
    else if (!operator.equals(SpanCompositionPlugin.PARAM_VALUE_OVERLAP))
      throw new PluginUserException("Invalid operation: " + operator);

    this.beginB = parts[9].trim();
    this.beginDirectionB = parts[10].trim();
    this.beginOffsetB = Integer.valueOf(parts[11].trim()).toString();

    this.endB = parts[12].trim();
    this.endDirectionB = parts[13].trim();
    this.endOffsetB = Integer.valueOf(parts[14].trim()).toString();

    this.expectedOutput = (parts.length == 15)
      ? new String[0]
      : parts[15].split("\\s*,\\s*");
  }
}
