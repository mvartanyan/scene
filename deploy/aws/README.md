# SCENE staging storage

`scene-staging-storage.yaml` defines the retained AWS resources for SCENE staging:

- DynamoDB table `scene-staging` with `pk`/`sk` and `gsi1`/`gsi2`/`gsi3`;
- a generated-name, private, encrypted, versioned S3 bucket;
- a retained bucket policy denying plaintext transport and TLS below 1.2;
- an IAM user and attached least-privilege policy for the app and dispatcher;
- exact-prefix version listing and object-version deletion so run cleanup
  physically removes retained S3 versions and delete markers;
- no IAM access key, password, Kubernetes Secret, or other credential material.

The fixed deployment region is `eu-central-1` and the fixed stack name is
`scene-staging-storage`.

## Static validation

Run the repository-local semantic check before contacting AWS:

```sh
scripts/scene_aws_validate.rb
```

`cfn-lint deploy/aws/scene-staging-storage.yaml` should also be run when
`cfn-lint` is available. The local validator intentionally does not contact AWS.

## Change set and deployment

Use a named CloudFormation change set and inspect it before execution. Stop if
the table or bucket would be replaced, deleted, or made public, or if the IAM
policy becomes broader than the template in this directory.

```sh
export AWS_REGION=eu-central-1
export STACK_NAME=scene-staging-storage
export CHANGE_SET="scene-staging-storage-$(date -u +%Y%m%dT%H%M%SZ)"

aws cloudformation create-change-set \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --change-set-name "$CHANGE_SET" \
  --change-set-type CREATE \
  --capabilities CAPABILITY_NAMED_IAM \
  --template-body file://deploy/aws/scene-staging-storage.yaml

aws cloudformation wait change-set-create-complete \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --change-set-name "$CHANGE_SET"

aws cloudformation describe-change-set \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --change-set-name "$CHANGE_SET"
```

Use `--change-set-type UPDATE` when the stack already exists. Execute only after
reviewing the complete change set:

```sh
aws cloudformation execute-change-set \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --change-set-name "$CHANGE_SET"

aws cloudformation wait stack-create-complete \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME"
```

For an update, wait with `stack-update-complete`. Record only non-secret outputs:

```sh
aws cloudformation describe-stacks \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].{Id:StackId,Status:StackStatus,Outputs:Outputs}'
```

Before strict k3s validation, replace
`REPLACE_WITH_CLOUDFORMATION_ARTIFACT_BUCKET_NAME` in
`deploy/k3s/configmap.yaml` with the recorded `ArtifactBucketName`. The bucket
name is operational configuration, not a credential; do not put it in the AWS
credential Secret.

## Storage verification

Verify controls before creating credentials or importing data:

```sh
aws dynamodb describe-continuous-backups \
  --region eu-central-1 \
  --table-name scene-staging

aws s3api get-public-access-block --bucket "$SCENE_S3_BUCKET"
aws s3api get-bucket-encryption --bucket "$SCENE_S3_BUCKET"
aws s3api get-bucket-versioning --bucket "$SCENE_S3_BUCKET"
```

The bucket, bucket policy, and table use `DeletionPolicy: Retain` and
`UpdateReplacePolicy: Retain`; the table also has deletion protection. Do not
remove those controls for routine rollback or Kubernetes teardown.

## Credential rotation

Create an access key only after the stack and Kubernetes namespace exist. Keep
shell tracing disabled. Pipe the AWS response directly into a Kubernetes Secret;
do not write it to a file or paste it into a command, ticket, terminal transcript,
or Git:

```sh
set +x
IAM_USER=scene-staging-app

aws iam create-access-key --user-name "$IAM_USER" --output json |
  ruby -rjson -e 'd=JSON.parse(STDIN.read).fetch("AccessKey"); puts "AWS_ACCESS_KEY_ID=#{d.fetch("AccessKeyId")}"; puts "AWS_SECRET_ACCESS_KEY=#{d.fetch("SecretAccessKey")}"' |
  kubectl --kubeconfig /Users/michael/.kube/config-horse -n scene \
    create secret generic scene-app-aws \
    --from-env-file=/dev/stdin --dry-run=client -o yaml |
  kubectl --kubeconfig /Users/michael/.kube/config-horse apply -f -
```

Restart and verify both Deployments before deleting the previous key. Use
`aws iam list-access-keys --user-name "$IAM_USER"` to identify key IDs; access
key IDs are identifiers, not secret values. Keep at most two keys during the
short rotation overlap, then delete the old key with `aws iam delete-access-key`.

## Teardown

Teardown requires explicit approval. Delete all IAM access keys before deleting
the CloudFormation stack. Stack deletion removes the IAM user/policy but retains
the DynamoDB table, S3 bucket, and its transport-deny policy. Emptying or deleting
retained storage or removing that policy is a separate destructive operation and
is not part of normal teardown.
