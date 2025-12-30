# DynamoDB Tables CDK Configuration

This document contains all the information needed to recreate the DynamoDB tables using AWS CDK.

## DynamoDB Tables

### 1. cat-test-certification-session-tokens

**Configuration:**
- **Table Name**: cat-test-certification-session-tokens
- **Partition Key**: documento (String)
- **Billing Mode**: PAY_PER_REQUEST (On-Demand)
- **Table Class**: STANDARD
- **Deletion Protection**: Disabled
- **Current Size**: 175 bytes
- **Item Count**: 3 items

**Warm Throughput:**
- Read Units Per Second: 12,000
- Write Units Per Second: 4,000

**CDK Code:**
```typescript
const sessionTokensTable = new dynamodb.Table(this, 'SessionTokensTable', {
  tableName: 'cat-env-certification-session-tokens', //according to env
  partitionKey: {
    name: 'documento',
    type: dynamodb.AttributeType.STRING
  },
  billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
  tableClass: dynamodb.TableClass.STANDARD,
  deletionProtection: false,
  warmThroughput: {
    readUnitsPerSecond: 12000,
    writeUnitsPerSecond: 4000
  }
});
```

---

### 2. cat-test-certification-data

**Configuration:**
- **Table Name**: cat-test-certification-data
- **Partition Key**: id (String)
- **Billing Mode**: PAY_PER_REQUEST (On-Demand)
- **Table Class**: STANDARD
- **Deletion Protection**: Disabled
- **Current Size**: 38,276 bytes
- **Item Count**: 203 items

**Warm Throughput:**
- Read Units Per Second: 12,000
- Write Units Per Second: 4,000

**CDK Code:**
```typescript
const certificationDataTable = new dynamodb.Table(this, 'CertificationDataTable', {
  tableName: 'cat-env-certification-data', // according to env
  partitionKey: {
    name: 'id',
    type: dynamodb.AttributeType.STRING
  },
  billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
  tableClass: dynamodb.TableClass.STANDARD,
  deletionProtection: false,
  warmThroughput: {
    readUnitsPerSecond: 12000,
    writeUnitsPerSecond: 4000
  }
});
```

## Required CDK Imports

```typescript
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
```

## Complete Stack Example

```typescript
import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';

export class DynamoDbStack extends cdk.Stack {
  public readonly sessionTokensTable: dynamodb.Table;
  public readonly certificationDataTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Session Tokens Table
    this.sessionTokensTable = new dynamodb.Table(this, 'SessionTokensTable', {
      tableName: 'cat-env-certification-session-tokens', //according to env
      partitionKey: {
        name: 'documento',
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableClass: dynamodb.TableClass.STANDARD,
      deletionProtection: false,
      warmThroughput: {
        readUnitsPerSecond: 12000,
        writeUnitsPerSecond: 4000
      }
    });

    // Certification Data Table
    this.certificationDataTable = new dynamodb.Table(this, 'CertificationDataTable', {
      tableName: 'cat-env-certification-data', //according to env
      partitionKey: {
        name: 'id',
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      tableClass: dynamodb.TableClass.STANDARD,
      deletionProtection: false,
      warmThroughput: {
        readUnitsPerSecond: 12000,
        writeUnitsPerSecond: 4000
      }
    });
  }
}
```

## Table Access Patterns

### Session Tokens Table
- **Primary Access**: Query by documento (partition key)
- **Use Case**: Store and retrieve session tokens for user authentication
- **Key Pattern**: documento = user document ID

### Certification Data Table
- **Primary Access**: Query by id (partition key)
- **Use Case**: Store certification request data and status
- **Key Pattern**: id = unique certification request identifier

## Notes

- Both tables use **PAY_PER_REQUEST** billing mode for cost optimization
- **Warm throughput** is configured for both tables (12K read, 4K write units/sec)
- Tables use **STANDARD** table class
- **Deletion protection** is disabled (enable for production)
- Both tables have simple single-attribute partition keys
- No sort keys or secondary indexes are configured
- Consider adding **point-in-time recovery** for production environments

## Production Considerations

```typescript
// Add point-in-time recovery for production
pointInTimeRecovery: true,

// Enable deletion protection for production
deletionProtection: true,

// Add tags for resource management
tags: {
  ProjectId: 'Catastro-Certificaciones',
  Client : 'CAT'
  Env : 'env',
  ManagedBy: 'CDK'
}
```
