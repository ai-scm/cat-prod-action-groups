# Lambda Functions CDK Configuration

This document contains all the information needed to recreate the Lambda functions using AWS CDK.

## Common Infrastructure

### VPC Configuration
- **VPC ID**: vpc-0948ca63d41f2050e
- **Subnet ID**: subnet-0a935b8953363aece
- **Security Group ID**: sg-00a688cb02b18032f

### Lambda Layer
- **Layer Name**: requests-layer
- **Layer ARN**: arn:aws:lambda:us-east-1:081899001252:layer:requests-layer:2
- **Description**: Python 3.13 compatible runtime
- **Compatible Runtimes**: python3.13, python3.12, python3.11, python3.10
- **Compatible Architectures**: x86_64
- **Code Size**: 1,069,502 bytes

## Lambda Functions

### 1. cat-prod-lambda-buscar-predios

**Configuration:**
- **Runtime**: python3.12
- **Handler**: buscar_predios.handler
- **Timeout**: 420 seconds (7 minutes)
- **Memory**: 128 MB
- **Architecture**: x86_64
- **Code Size**: 11,785 bytes

**Environment Variables:**
- ENABLE_MOCK: "false"

**IAM Role**: arn:aws:iam::081899001252:role/cat-prod-lambda-buscar-predios-role

**CDK Code:**
```typescript
const buscarPrediosFunction = new lambda.Function(this, 'BuscarPrediosFunction', {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: 'buscar_predios.handler',
  code: lambda.Code.fromAsset('lambda/buscar-predios'),
  timeout: Duration.seconds(420),
  memorySize: 128,
  architecture: lambda.Architecture.X86_64,
  environment: {
    ENABLE_MOCK: 'false'
  },
  vpc: vpc,
  vpcSubnets: {
    subnets: [subnet]
  },
  securityGroups: [securityGroup],
  layers: [requestsLayer],
  logGroup: new logs.LogGroup(this, 'BuscarPrediosLogGroup', {
    logGroupName: '/aws/lambda/cat-prod-lambda-buscar-predios',
    retention: logs.RetentionDays.ONE_WEEK
  })
});
```

---

### 2. cat-prod-lambda-contar-predios

**Configuration:**
- **Runtime**: python3.13
- **Handler**: lambda_function.lambda_handler
- **Timeout**: 360 seconds (6 minutes)
- **Memory**: 128 MB
- **Architecture**: x86_64
- **Code Size**: 7,990 bytes

**Environment Variables:**
- ENABLE_MOCK: "false"

**IAM Role**: arn:aws:iam::081899001252:role/service-role/cat-prod-lambda-contar-predios-role-qoy7cm7b

**CDK Code:**
```typescript
const contarPrediosFunction = new lambda.Function(this, 'ContarPrediosFunction', {
  runtime: lambda.Runtime.PYTHON_3_13,
  handler: 'lambda_function.lambda_handler',
  code: lambda.Code.fromAsset('lambda/contar-predios'),
  timeout: Duration.seconds(360),
  memorySize: 128,
  architecture: lambda.Architecture.X86_64,
  environment: {
    ENABLE_MOCK: 'false'
  },
  vpc: vpc,
  vpcSubnets: {
    subnets: [subnet]
  },
  securityGroups: [securityGroup],
  layers: [requestsLayer],
  logGroup: new logs.LogGroup(this, 'ContarPrediosLogGroup', {
    logGroupName: '/aws/lambda/cat-prod-lambda-contar-predios',
    retention: logs.RetentionDays.ONE_WEEK
  })
});
```

---

### 3. cat-prod-lambda-generar-certificados

**Configuration:**
- **Runtime**: python3.12
- **Handler**: generar_certificados.handler
- **Timeout**: 450 seconds (7.5 minutes)
- **Memory**: 256 MB
- **Architecture**: x86_64
- **Code Size**: 12,246 bytes
- **Description**: "Genera y envía certificados por correo"

**Environment Variables:**
- ENABLE_MOCK: "false"

**IAM Role**: arn:aws:iam::081899001252:role/cat-prod-certificaciones--GenerarCertsFnServiceRole-fH46yHO7YAWS

**Tags:**
- ProjectId: P2124
- Client: CAT
- ManagedBy: CDK
- Environment: env
- Purpose: bedrock-agent-action
- Component: lambda-function

**CDK Code:**
```typescript
const generarCertificadosFunction = new lambda.Function(this, 'GenerarCertificadosFunction', {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: 'generar_certificados.handler',
  code: lambda.Code.fromAsset('lambda/generar-certificados'),
  timeout: Duration.seconds(450),
  memorySize: 256,
  architecture: lambda.Architecture.X86_64,
  description: 'Genera y envía certificados por correo',
  environment: {
    ENABLE_MOCK: 'false'
  },
  vpc: vpc,
  vpcSubnets: {
    subnets: [subnet]
  },
  securityGroups: [securityGroup],
  layers: [requestsLayer],
  logGroup: new logs.LogGroup(this, 'GenerarCertificadosLogGroup', {
    logGroupName: '/aws/lambda/cat-prod-lambda-generar-certificados',
    retention: logs.RetentionDays.ONE_WEEK
  })
});

Tags.of(generarCertificadosFunction).add('ProjectId', 'P2124');
Tags.of(generarCertificadosFunction).add('ManagedBy', 'CDK');
Tags.of(generarCertificadosFunction).add('Env', 'env'); //according to config
Tags.of(generarCertificadosFunction).add('Purpose', 'bedrock-agent-action');
Tags.of(generarCertificadosFunction).add('Component', 'lambda-function');
```

---

### 4. cat-prod-lambda-listar-predios

**Configuration:**
- **Runtime**: python3.12
- **Handler**: lambda_function.handler
- **Timeout**: 420 seconds (7 minutes)
- **Memory**: 128 MB
- **Architecture**: x86_64
- **Code Size**: 9,599 bytes

**Environment Variables:**
- ENABLE_MOCK: "false"

**IAM Role**: arn:aws:iam::081899001252:role/cat-prod-certificaciones--GenerarCertsFnServiceRole-fH46yHO7YAWS

**CDK Code:**
```typescript
const listarPrediosFunction = new lambda.Function(this, 'ListarPrediosFunction', {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: 'lambda_function.handler',
  code: lambda.Code.fromAsset('lambda/listar-predios'),
  timeout: Duration.seconds(420),
  memorySize: 128,
  architecture: lambda.Architecture.X86_64,
  environment: {
    ENABLE_MOCK: 'false'
  },
  vpc: vpc,
  vpcSubnets: {
    subnets: [subnet]
  },
  securityGroups: [securityGroup],
  layers: [requestsLayer],
  logGroup: new logs.LogGroup(this, 'ListarPrediosLogGroup', {
    logGroupName: '/aws/lambda/cat-prod-lambda-listar-predios',
    retention: logs.RetentionDays.ONE_WEEK
  })
});
```

---

### 5. cat-prod-lambda-validar-identidad

**Configuration:**
- **Runtime**: python3.12
- **Handler**: validar_identidad.lambda_handler
- **Timeout**: 600 seconds (10 minutes)
- **Memory**: 256 MB
- **Architecture**: x86_64
- **Code Size**: 5,152 bytes
- **Description**: "Valida la identidad del ciudadano mediante documento"

**Environment Variables:**
- ENABLE_MOCK: "false"

**IAM Role**: arn:aws:iam::081899001252:role/cat-prod-certificaciones--ValidarIdentidadFnService-Gt4PuBQ7FjrL

**Layer**: requests-layer:1 (different version)

**Tags:**
- ProjectId: P2124
- Client: CAT
- ManagedBy: CDK
- Environment: env
- Purpose: bedrock-agent-action
- Component: lambda-function

**CDK Code:**
```typescript
const validarIdentidadFunction = new lambda.Function(this, 'ValidarIdentidadFunction', {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: 'validar_identidad.lambda_handler',
  code: lambda.Code.fromAsset('lambda/validar-identidad'),
  timeout: Duration.seconds(600),
  memorySize: 256,
  architecture: lambda.Architecture.X86_64,
  description: 'Valida la identidad del ciudadano mediante documento',
  environment: {
    ENABLE_MOCK: 'false'
  },
  vpc: vpc,
  vpcSubnets: {
    subnets: [subnet]
  },
  securityGroups: [securityGroup],
  layers: [requestsLayer], // Note: uses version 1 in production
  logGroup: new logs.LogGroup(this, 'ValidarIdentidadLogGroup', {
    logGroupName: '/aws/lambda/cat-prod-lambda-validar-identidad',
    retention: logs.RetentionDays.ONE_WEEK
  })
});

Tags.of(generarCertificadosFunction).add('ProjectId', 'P2124');
Tags.of(generarCertificadosFunction).add('ManagedBy', 'CDK');
Tags.of(generarCertificadosFunction).add('Env', 'env'); //according to config
Tags.of(generarCertificadosFunction).add('Purpose', 'bedrock-agent-action');
Tags.of(generarCertificadosFunction).add('Component', 'lambda-function');
```

---

### 6. cat-prod-lambda-validar-otp

**Configuration:**
- **Runtime**: python3.12
- **Handler**: validar_otp.handler
- **Timeout**: 300 seconds (5 minutes)
- **Memory**: 256 MB
- **Architecture**: x86_64
- **Code Size**: 7,336 bytes
- **Description**: "Valida el código OTP ingresado"

**Environment Variables:**
- ENABLE_MOCK: "false"

**IAM Role**: arn:aws:iam::081899001252:role/cat-prod-certificaciones--ValidarOTPFnServiceRole74-7CcanYpya1R6

**Layer**: requests-layer:1 (different version)

**Tags:**
- ProjectId: P2124
- Client: CAT
- ManagedBy: CDK
- Environment: env
- Purpose: bedrock-agent-action
- Component: lambda-function


**CDK Code:**
```typescript
const validarOtpFunction = new lambda.Function(this, 'ValidarOtpFunction', {
  runtime: lambda.Runtime.PYTHON_3_12,
  handler: 'validar_otp.handler',
  code: lambda.Code.fromAsset('lambda/validar-otp'),
  timeout: Duration.seconds(300),
  memorySize: 256,
  architecture: lambda.Architecture.X86_64,
  description: 'Valida el código OTP ingresado',
  environment: {
    ENABLE_MOCK: 'false'
  },
  vpc: vpc,
  vpcSubnets: {
    subnets: [subnet]
  },
  securityGroups: [securityGroup],
  layers: [requestsLayer], // Note: uses version 1 in production
  logGroup: new logs.LogGroup(this, 'ValidarOtpLogGroup', {
    logGroupName: '/aws/lambda/cat-prod-lambda-validar-otp',
    retention: logs.RetentionDays.ONE_WEEK
  })
});

Tags.of(generarCertificadosFunction).add('ProjectId', 'P2124');
Tags.of(generarCertificadosFunction).add('ManagedBy', 'CDK');
Tags.of(generarCertificadosFunction).add('Env', 'env'); //according to config
Tags.of(generarCertificadosFunction).add('Purpose', 'bedrock-agent-action');
Tags.of(generarCertificadosFunction).add('Component', 'lambda-function');
```

## Required CDK Imports

```typescript
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Duration, Tags } from 'aws-cdk-lib';
```

## Infrastructure Prerequisites

Before creating the Lambda functions, you'll need to create or reference:

1. **VPC and Networking**:
   ```typescript
   const vpc = ec2.Vpc.fromLookup(this, 'ExistingVpc', {
     vpcId: 'vpc-0948ca63d41f2050e'
   });
   
   const subnet = ec2.Subnet.fromSubnetId(this, 'ExistingSubnet', 'subnet-0a935b8953363aece');
   
   const securityGroup = ec2.SecurityGroup.fromSecurityGroupId(this, 'ExistingSecurityGroup', 'sg-00a688cb02b18032f');
   ```

2. **Lambda Layer**:
   ```typescript
   const requestsLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'RequestsLayer', 
     'arn:aws:lambda:us-east-1:081899001252:layer:requests-layer:2'
   );
   ```

3. **IAM Roles**: You'll need to create appropriate IAM roles with the necessary permissions for each function.

## Notes

- All functions use the same VPC configuration
- Most functions use the requests-layer:2, but validar-identidad and validar-otp use version 1
- Functions have different timeout and memory configurations based on their workload
- Some functions have specific tags for CloudFormation stack management
- All functions have the ENABLE_MOCK environment variable set to "false"
- Log groups are created with 1-week retention (adjust as needed)
