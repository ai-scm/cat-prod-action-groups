# Lambda IAM Roles CDK Configuration

This document contains all the information needed to recreate the Lambda IAM roles using AWS CDK.

## Common Policies

### DynamoDBReadAccess Policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DynamoDBReadAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:GetItem",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:us-east-1:081899001252:table/cat-test-certification-session-tokens"
    }
  ]
}
```

### VPCNetworkInterface Policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces"
      ],
      "Resource": "*"
    }
  ]
}
```

## Lambda IAM Roles

### 1. cat-prod-lambda-buscar-predios-role

**Configuration:**
- **Role Name**: cat-prod-lambda-buscar-predios-role
- **Path**: /
- **Description**: "Allows Lambda functions to call AWS services on your behalf."

**Attached Policies:**
- DynamoDBReadAccess (custom)
- VPCNetworkInterface (custom)

**CDK Code:**
```typescript
const buscarPrediosRole = new iam.Role(this, 'BuscarPrediosRole', {
  roleName: 'cat-prod-lambda-buscar-predios-role',
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  description: 'Allows Lambda functions to call AWS services on your behalf.',
  managedPolicies: [
    dynamoDbReadPolicy,
    vpcNetworkInterfacePolicy
  ]
});
```

---

### 2. cat-prod-lambda-contar-predios-role-qoy7cm7b

**Configuration:**
- **Role Name**: cat-prod-lambda-contar-predios-role-qoy7cm7b
- **Path**: /service-role/

**Attached Policies:**
- DynamoDBReadAccess (custom)
- VPCNetworkInterface (custom)
- AWSLambdaBasicExecutionRole (AWS managed)

**CDK Code:**
```typescript
const contarPrediosRole = new iam.Role(this, 'ContarPrediosRole', {
  roleName: 'cat-prod-lambda-contar-predios-role',
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  managedPolicies: [
    iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    dynamoDbReadPolicy,
    vpcNetworkInterfacePolicy
  ]
});
```

---

### 3. cat-prod-certificaciones--GenerarCertsFnServiceRole

**Configuration:**
- **Role Name**: cat-prod-certificaciones--GenerarCertsFnServiceRole
- **Path**: /

**Attached Policies:**
- AWSLambdaBasicExecutionRole (AWS managed)

**Inline Policies:**
- dynamo-policy
- MockTableOperations
- SESDynamoPolicy
- vpc_policy

**Tags:**
- Project: Catastro-Certificaciones
- Runtime: python3.12
- ManagedBy: CDK
- CostCenter: IT-Operations
- Service: certificaciones-catastrales
- Environment: Production
- Purpose: bedrock-agent-action
- Component: lambda-function
- StackName: cat-prod-certificaciones-agent-stack

**CDK Code:**
```typescript
const generarCertsRole = new iam.Role(this, 'GenerarCertsRole', {
  roleName: 'cat-prod-certificaciones--GenerarCertsFnServiceRole',
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  managedPolicies: [
    iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
  ],
  inlinePolicies: {
    'dynamo-policy': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'dynamodb:PutItem',
            'dynamodb:GetItem',
            'dynamodb:UpdateItem',
            'dynamodb:DeleteItem',
            'dynamodb:Query',
            'dynamodb:Scan'
          ],
          resources: [
            'arn:aws:dynamodb:us-east-1:*:table/cat-test-certification-data',
            'arn:aws:dynamodb:us-east-1:*:table/cat-test-certification-session-tokens'
          ]
        })
      ]
    }),
    'SESDynamoPolicy': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'ses:SendEmail',
            'ses:SendRawEmail'
          ],
          resources: ['*']
        })
      ]
    }),
    'vpc_policy': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'ec2:CreateNetworkInterface',
            'ec2:DeleteNetworkInterface',
            'ec2:DescribeNetworkInterfaces'
          ],
          resources: ['*']
        })
      ]
    }),
    'MockTableOperations': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'dynamodb:PutItem',
            'dynamodb:GetItem',
            'dynamodb:UpdateItem',
            'dynamodb:DeleteItem'
          ],
          resources: [
            'arn:aws:dynamodb:us-east-1:*:table/mock-*'
          ]
        })
      ]
    })
  }
});

Tags.of(generarCertsRole).add('Project', 'Catastro-Certificaciones');
Tags.of(generarCertsRole).add('Runtime', 'python3.12');
Tags.of(generarCertsRole).add('ManagedBy', 'CDK');
Tags.of(generarCertsRole).add('CostCenter', 'IT-Operations');
Tags.of(generarCertsRole).add('Service', 'certificaciones-catastrales');
Tags.of(generarCertsRole).add('Environment', 'Production');
Tags.of(generarCertsRole).add('Purpose', 'bedrock-agent-action');
Tags.of(generarCertsRole).add('Component', 'lambda-function');
```

---

### 4. cat-prod-certificaciones--ValidarIdentidadFnService

**Configuration:**
- **Role Name**: cat-prod-certificaciones--ValidarIdentidadFnService
- **Path**: /

**Attached Policies:**
- AWSLambdaBasicExecutionRole (AWS managed)

**Inline Policies:**
- MockTableOperations
- NetworkInterfaceForLambda
- SESSendMockEmails

**Tags:** (Same as GenerarCerts role)

**CDK Code:**
```typescript
const validarIdentidadRole = new iam.Role(this, 'ValidarIdentidadRole', {
  roleName: 'cat-prod-certificaciones--ValidarIdentidadFnService',
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  managedPolicies: [
    iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
  ],
  inlinePolicies: {
    'NetworkInterfaceForLambda': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'ec2:CreateNetworkInterface',
            'ec2:DeleteNetworkInterface',
            'ec2:DescribeNetworkInterfaces'
          ],
          resources: ['*']
        })
      ]
    }),
    'SESSendMockEmails': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'ses:SendEmail',
            'ses:SendRawEmail'
          ],
          resources: ['*']
        })
      ]
    }),
    'MockTableOperations': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'dynamodb:PutItem',
            'dynamodb:GetItem',
            'dynamodb:UpdateItem',
            'dynamodb:DeleteItem'
          ],
          resources: [
            'arn:aws:dynamodb:us-east-1:*:table/mock-*'
          ]
        })
      ]
    })
  }
});

// Add same tags as GenerarCerts role
```

---

### 5. cat-prod-certificaciones--ValidarOTPFnServiceRole

**Configuration:**
- **Role Name**: cat-prod-certificaciones--ValidarOTPFnServiceRole
- **Path**: /

**Attached Policies:**
- AWSLambdaBasicExecutionRole (AWS managed)

**Inline Policies:**
- DynamoDBAccessPolicy
- vpc_policy

**Tags:** (Same as GenerarCerts role)

**CDK Code:**
```typescript
const validarOtpRole = new iam.Role(this, 'ValidarOtpRole', {
  roleName: 'cat-prod-certificaciones--ValidarOTPFnServiceRole',
  assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
  managedPolicies: [
    iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
  ],
  inlinePolicies: {
    'DynamoDBAccessPolicy': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'dynamodb:PutItem',
            'dynamodb:GetItem',
            'dynamodb:UpdateItem',
            'dynamodb:DeleteItem',
            'dynamodb:Query',
            'dynamodb:Scan'
          ],
          resources: [
            'arn:aws:dynamodb:us-east-1:*:table/cat-test-certification-session-tokens'
          ]
        })
      ]
    }),
    'vpc_policy': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'ec2:CreateNetworkInterface',
            'ec2:DeleteNetworkInterface',
            'ec2:DescribeNetworkInterfaces'
          ],
          resources: ['*']
        })
      ]
    })
  }
});

// Add same tags as GenerarCerts role
```

## Required CDK Imports

```typescript
import * as iam from 'aws-cdk-lib/aws-iam';
import { Tags } from 'aws-cdk-lib';
```

## Common Policies Creation

```typescript
// Create reusable policies
const dynamoDbReadPolicy = new iam.ManagedPolicy(this, 'DynamoDBReadAccess', {
  managedPolicyName: 'DynamoDBReadAccess',
  description: 'Grant permission to read access token Dynamo Table',
  statements: [
    new iam.PolicyStatement({
      sid: 'DynamoDBReadAccess',
      effect: iam.Effect.ALLOW,
      actions: [
        'dynamodb:PutItem',
        'dynamodb:UpdateItem',
        'dynamodb:GetItem',
        'dynamodb:Query'
      ],
      resources: [
        'arn:aws:dynamodb:us-east-1:*:table/cat-test-certification-session-tokens'
      ]
    })
  ]
});

const vpcNetworkInterfacePolicy = new iam.ManagedPolicy(this, 'VPCNetworkInterface', {
  managedPolicyName: 'VPCNetworkInterface',
  statements: [
    new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'ec2:CreateNetworkInterface',
        'ec2:DeleteNetworkInterface',
        'ec2:DescribeNetworkInterfaces'
      ],
      resources: ['*']
    })
  ]
});
```

## Complete Stack Example

```typescript
export class LambdaRolesStack extends cdk.Stack {
  public readonly buscarPrediosRole: iam.Role;
  public readonly contarPrediosRole: iam.Role;
  public readonly generarCertsRole: iam.Role;
  public readonly validarIdentidadRole: iam.Role;
  public readonly validarOtpRole: iam.Role;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create common policies first
    const dynamoDbReadPolicy = new iam.ManagedPolicy(this, 'DynamoDBReadAccess', {
      // ... policy definition
    });

    const vpcNetworkInterfacePolicy = new iam.ManagedPolicy(this, 'VPCNetworkInterface', {
      // ... policy definition
    });

    // Create all roles
    this.buscarPrediosRole = new iam.Role(this, 'BuscarPrediosRole', {
      // ... role definition
    });

    // ... other roles
  }
}
```

## Notes

- All roles assume the Lambda service principal
- Most roles include VPC network interface permissions for VPC Lambda execution
- DynamoDB permissions vary by function requirements
- SES permissions are included for email-sending functions
- Mock table operations are included for testing environments
- Tags are consistently applied to certificaciones-related roles
- Consider using least privilege principle and adjust permissions as needed
