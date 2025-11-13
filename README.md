# S3 Deduplication Lambda

This lambda deduplicates S3 messages.

## File Event Processing

```mermaid
flowchart LR
    SQS[SQS<br>multiple S3 Events fa:fa-copy] -- for each<br>S3 Event --> sqsproc
    subgraph sqsproc [S3 Event Processing]
        S3E[S3 Event] --> SB{Seen<br>Before}

        SB -- Yes -->F{Forwarded<br>Successfully}
        F -- No -->Lock
        F -- Yes<br>(success) -->Done

        SB -- No --> Lock[fa:fa-lock Lock]
        Lock -- already locked --> wfl[fa:fa-clock Wait For Lock]
        wfl --> Lock

        Lock -- lock acquired --> Forward{Attempt<br>Forward}
        Forward -- Success --> SS[Save Success]
        SS -- (success) --> Done
        Forward -- Fail --> SF[Save Failure]
        SF -- (failure) --> Done[S3 Event<br>Done]

        Lock -- lock failed --> SF
    end
    sqsproc -- "fa:fa-hourglass" --> all_success{all succeeded}
    all_success -- No --> rf[Lambda Returns<br>Failure]
    all_success -- Yes --> rs[Lambda Returns<br>Success]
    rf --> sqsd[SQS Done]
    rs --> sqsd
```

## building

Set the following variables before continuing:

```bash
export aws_region=us-west-2
export aws_acct=9XX9
export s3dedup_image_name=your-prefix/s3-event-deduplicator
export s3dedup_version=v5

```

### build docker image

```bash
uv lock
docker buildx build --platform linux/amd64 --provenance=false -t s3-event-deduplicator:${s3dedup_version} .
```

### Push image to ECR

Note: this assume docker is logged into the ECR. If not, see the "log docker into ECR" section below.

```bash
# tag for upload
docker tag s3-event-deduplicator:${s3dedup_version} ${aws_acct}.dkr.ecr.${aws_region}.amazonaws.com/${s3dedup_image_name}:${s3dedup_version}
# push to ECR:
docker push ${aws_acct}.dkr.ecr.${aws_region}.amazonaws.com/${s3dedup_image_name}:${s3dedup_version}
echo uploaded ${s3dedup_image_name} to ${aws_acct}.dkr.ecr.${aws_region}.amazonaws.com/${s3dedup_image_name}:${s3dedup_version}
```

### log docker into ECR

```bash
aws ecr get-login-password --region ${aws_region} | docker login --username AWS --password-stdin ${aws_acct}$.dkr.ecr.${aws_region}.amazonaws.com
```
