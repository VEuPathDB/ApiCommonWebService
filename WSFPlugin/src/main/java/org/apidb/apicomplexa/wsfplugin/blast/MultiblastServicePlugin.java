package org.apidb.apicomplexa.wsfplugin.blast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apidb.apicommon.model.TranscriptUtil;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.question.Question;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.DelayedResultException;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

public class MultiblastServicePlugin extends AbstractPlugin {

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] {
      "BlastDatabaseType",
      "BlastAlgorithm",
      "BlastDatabaseOrganism",
      "BlastQuerySequence",
      "BlastRecordClass", // TODO: Do we need this param for multi-blast or can we get from BlastDatabaseType?
      "ExpectationValue",
      "NumQueryResults",
      "MaxMatchesQueryRange",
      "WordSize",
      "ScoringMatrix",
      "MatchMismatchScore",
      "GapCosts",
      "CompAdjust",
      "FilterLowComplex",
      "SoftMask",
      "LowerCaseMask"
    };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    List<String> columns = new ArrayList<>();
    String questionName = request.getContext().get(Utilities.QUERY_CTX_QUESTION);
    WdkModel model = InstanceManager.getInstance(WdkModel.class, request.getProjectId());
    Question q = model.getQuestionByFullName(questionName).get();
    columns.addAll(Arrays.asList(q.getRecordClass().getPrimaryKeyDefinition().getColumnRefs()));
    if (TranscriptUtil.isTranscriptQuestion(q)) {
      columns.add("matched_result");
    }
    columns.addAll(Arrays.asList(new String[] {
      "summary",
      "alignment",
      "evalue_mant",
      "evalue_exp",
      "score"
    }));
    return columns.toArray(new String[0]);
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // let WDK handle validation for now
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {

    return 0;
  }

}
