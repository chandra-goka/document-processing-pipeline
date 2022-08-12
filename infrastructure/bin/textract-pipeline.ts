#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { MetadataStack } from '../lib/metadata-stack';
import { TextractPipelineStack } from '../lib/textract-pipeline-stack';

const app = new cdk.App();
const metadatastack = new MetadataStack(app, 'MetadataStack');
const textractstack = new TextractPipelineStack(app, 'TextractPipelineStack', {
    pipelineOpsTable : metadatastack.pipelineOpsTable,
    lineageTable : metadatastack.lineageTable,
    indexName : metadatastack.indexName,
    documentRegistryTable : metadatastack.documentRegistryTable,
    lineageSQS : metadatastack.lineageSQS,
    pipelineOpsSQS : metadatastack.pipelineOpsSQS,
    documentRegistrySQS : metadatastack.documentRegistrySQS,
    metadataTopic : metadatastack.metadataTopic,
});
