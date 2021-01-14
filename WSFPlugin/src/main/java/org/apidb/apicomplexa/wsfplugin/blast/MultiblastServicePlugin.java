package org.apidb.apicomplexa.wsfplugin.blast;

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

  /* TODO: add these as params and cols

   <!--The order of these params has changed for multi-blast. Thus the param query refs need to change accordingly-->
   <paramRef ref="sharedParams.BlastDatabaseType" quote="false" noTranslation="false" default="Transcripts"/>
   <paramRef ref="sharedParams.BlastAlgorithm" quote="false" noTranslation="false" />
   <paramRef ref="sharedParams.BlastDatabaseOrganism" quote="false" noTranslation="false" default="%%primaryOrthoOrganism%%" />
   <paramRef ref="sharedParams.BlastQuerySequence"/>

   <!--Do we need this param for multi-blast or can we get from BlastDatabaseType?-->
   <paramRef ref="sharedParams.BlastRecordClass" default="TranscriptRecordClasses.TranscriptRecordClass" />

   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.ExpectationValue"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.NumQueryResults"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.MaxMatchesQueryRange"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.WordSize"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.ScoringMatrix"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.MatchMismatchScore"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.GapCosts"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.CompAdjust"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.FilterLowComplex"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.SoftMask"/>
   <paramRef groupRef="paramGroups.advancedParams" ref="sharedParams.LowerCaseMask"/>

    <wsColumn name="source_id" width="50" wsName="identifier"/>
    <wsColumn name="gene_source_id" width="50" wsName="gene_source_id"/>
    <wsColumn name="project_id" width="20" />
    <wsColumn name="matched_result" width="1" wsName="matched_result"/>
    <wsColumn name="summary" width="3000"/>
    <wsColumn name="alignment" columnType="clob"/>
    <wsColumn name="evalue_mant" columnType="float" />
    <wsColumn name="evalue_exp" columnType="number" />
    <wsColumn name="score" columnType="float" />

  */

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { "delayResult" };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    String questionName = request.getContext().get(Utilities.QUERY_CTX_QUESTION);
    WdkModel model = InstanceManager.getInstance(WdkModel.class, request.getProjectId());
    Question q = model.getQuestionByFullName(questionName).get();
    return q.getRecordClass().getPrimaryKeyDefinition().getColumnRefs();
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // nothing to do here
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {
    String delayResultVal = request.getParams().get("delayResult");
    boolean delayResult = delayResultVal != null && delayResultVal.equals("true");
    if (delayResult) {
      throw new DelayedResultException();
    }
    return 0;
  }

}
