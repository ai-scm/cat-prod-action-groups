# Bedrock Agent CDK Configuration

This document contains all the information needed to recreate the Bedrock agent using AWS CDK.

## Agent Overview

### Basic Configuration
- **Agent ID**: 63ZQAGX043
- **Agent Name**: cat-test-certificaciones-agent
- **Agent ARN**: arn:aws:bedrock:us-east-1:081899001252:agent/63ZQAGX043
- **Status**: PREPARED
- **Foundation Model**: anthropic.claude-3-5-sonnet-20240620-v1:0
- **Description**: "Agente para solicitudes de certificaciones catastrales."
- **Idle Session TTL**: 600 seconds (10 minutes)
- **Agent Collaboration**: DISABLED
- **Orchestration Type**: DEFAULT

### Agent Resource Role
- **Role ARN**: arn:aws:iam::081899001252:role/service-role/AmazonBedrockExecutionRoleForAgents_HT6S89GWSBL

## Action Groups

### 1. ValidarIdentidad
- **Action Group ID**: FDWDSM3MG7
- **State**: ENABLED
- **Description**: "Valida la identidad de un ciudadano mediante su número de cédula"
- **Lambda Function**: arn:aws:lambda:us-east-1:081899001252:function:cat-prod-lambda-validar-identidad

### 2. ValidarOTP
- **Action Group ID**: KFLKSJIS5X
- **State**: ENABLED
- **Description**: "Valida el código de verificación (OTP)."
- **Lambda Function**: arn:aws:lambda:us-east-1:081899001252:function:cat-prod-lambda-validar-otp

### 3. ContarPredios
- **Action Group ID**: LJSRXDAZ8A
- **State**: ENABLED
- **Description**: "Action Group to perform calls to get the users numbers of properties data"
- **Lambda Function**: arn:aws:lambda:us-east-1:081899001252:function:cat-prod-lambda-contar-predios

### 4. listarPredios
- **Action Group ID**: LPAEVFA5CW
- **State**: ENABLED
- **Description**: "Muestra la lista de predios del usuario cuando tenga entre 1 y 10 registrados."
- **Lambda Function**: arn:aws:lambda:us-east-1:081899001252:function:cat-prod-lambda-listar-predios

### 5. buscarPredios
- **Action Group ID**: VC77HXJVWJ
- **State**: ENABLED
- **Description**: "Busca predios específicos usando CHIP, dirección o matrícula."
- **Lambda Function**: arn:aws:lambda:us-east-1:081899001252:function:cat-prod-lambda-buscar-predios

### 6. GenerarCertificados
- **Action Group ID**: S9CWXYCUXX
- **State**: ENABLED
- **Description**: "Envía solicitud de generación de certificados a la API."
- **Lambda Function**: arn:aws:lambda:us-east-1:081899001252:function:cat-prod-lambda-generar-certificados

### 7. UserInputAction
- **Action Group ID**: Z10KRMOKED
- **State**: ENABLED
- **Description**: (No description provided)

## Agent Aliases

### Active Aliases
1. **noNameAgent** (VWNDI586X0) - Version 22
2. **Alias_22Dic_Fix** (SWOV7ZHAHS) - Version 20
3. **22_Dic_test** (BWYLDNANHC) - Version 19
4. **test2_18DIC** (L8GOKFM7LO) - Version 18
5. **12_dic_alias_test** (BDWQZNIGH0) - Version 14
6. **Test_prompt_corto_v2** (T4IXSJB2WG) - Version 12
7. **Test_prompt_corto** (DLUXDHVNRT) - Version 11
8. **AgentTestAlias** (TSTALIASID) - DRAFT version

## CDK Implementation

### Required CDK Imports
```typescript
import * as bedrock from 'aws-cdk-lib/aws-bedrock';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
```

### Agent Role Creation
```typescript
const agentRole = new iam.Role(this, 'BedrockAgentRole', {
  roleName: 'AmazonBedrockExecutionRoleForAgents_CertificacionesAgent',
  assumedBy: new iam.ServicePrincipal('bedrock.amazonaws.com'),
  managedPolicies: [
    iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonBedrockAgentBedrockFoundationModelPolicy')
  ],
  inlinePolicies: {
    'InvokeLambdaPolicy': new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ['lambda:InvokeFunction'],
          resources: [
            buscarPrediosFunction.functionArn,
            contarPrediosFunction.functionArn,
            generarCertificadosFunction.functionArn,
            listarPrediosFunction.functionArn,
            validarIdentidadFunction.functionArn,
            validarOtpFunction.functionArn
          ]
        })
      ]
    })
  }
});
```

### Agent Creation
```typescript
const certificacionesAgent = new bedrock.CfnAgent(this, 'CertificacionesAgent', {
  agentName: 'cat-test-certificaciones-agent',
  description: 'Agente para solicitudes de certificaciones catastrales.',
  agentResourceRoleArn: agentRole.roleArn,
  foundationModel: 'anthropic.claude-3-5-sonnet-20240620-v1:0',
  idleSessionTtlInSeconds: 600,
  instruction: `<role> Eres el Asistente de Certificaciones Catastrales de la Unidad Administrativa Especial de Catastro Distrital (UAECD) de Bogotá. Tu función es guiar a los ciudadanos a través del proceso COMPLETO de solicitud de certificados catastrales de manera ordenada, profesional y determinista. </role>

<communication_style>
- Idioma: Español colombiano únicamente
- Tono: Profesional, claro y empático
- Formato: Respuestas concisas y directas
- Evita tecnicismos innecesarios
- Usa formato legible con saltos de línea cuando muestres información estructurada
- No uses sesgos de género en ningún contexto. NUNCA te refieras a ti mismo en femenino o masculino (usa "Asistente Virtual" o "Soy CatIA").
- REGLA DE LIMITES: Si te preguntan cuántos certificados se pueden generar, responde SIEMPRE que el límite es de 3 certificados diarios por usuario.
</communication_style>

<scope>
<permitted_actions>
Tu ÚNICO propósito es facilitar la solicitud de certificaciones catastrales siguiendo este flujo OBLIGATORIO y SECUENCIAL:
1. Aceptación de Tratamiento de Datos (Habeas Data)
2. Validación de Identidad del Ciudadano
3. Validación del Código OTP
4. Consulta de Predios Disponibles del usuario
5. Listar Predios Disponibles o Buscar Predios (según cantidad)
6. Selección de Certificados por el Usuario
7. Validación de la Selección
8. Generación y Envío de Certificados
</permitted_actions>

<prohibited_actions>
- NO debes responder consultas ajenas al proceso de certificaciones
- NO puedes saltarte pasos del flujo secuencial
- NO puedes proceder sin validaciones exitosas
- NO debes inventar información que no proviene de las Action Groups
- NO puedes retroceder en el flujo una vez avanzado un paso exitosamente
</prohibited_actions>
</scope>

[... rest of the instruction content ...]`,
  actionGroups: [
    {
      actionGroupName: 'ValidarIdentidad',
      description: 'Valida la identidad de un ciudadano mediante su número de cédula',
      actionGroupExecutor: {
        lambda: validarIdentidadFunction.functionArn
      },
      actionGroupState: 'ENABLED',
      apiSchema: {
        payload: JSON.stringify({
          openapi: '3.0.0',
          info: {
            title: 'Validar Identidad API',
            version: '1.0.0',
            description: 'API para validar la identidad de un ciudadano'
          },
          paths: {
            '/validar-identidad': {
              post: {
                summary: 'Valida el documento de identidad de un ciudadano',
                description: 'Verifica si el documento de identidad es válido y retorna información del ciudadano',
                operationId: 'validarIdentidad',
                requestBody: {
                  required: true,
                  content: {
                    'application/json': {
                      schema: {
                        type: 'object',
                        properties: {
                          documento: {
                            type: 'string',
                            description: 'Número de cédula de ciudadanía'
                          },
                          tipoDocumento: {
                            type: 'string',
                            description: 'Tipo de documento de identidad'
                          }
                        },
                        required: ['documento', 'tipoDocumento']
                      }
                    }
                  }
                },
                responses: {
                  '200': {
                    description: 'Respuesta de validación',
                    content: {
                      'application/json': {
                        schema: {
                          type: 'object',
                          properties: {
                            valido: { type: 'boolean' },
                            correo: { type: 'string' },
                            correo_ofuscado: { type: 'string' },
                            message: { type: 'string' }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        })
      }
    },
    // ... other action groups with similar structure
  ]
});
```

### Action Groups Configuration

Each action group follows this pattern:
```typescript
{
  actionGroupName: 'ActionGroupName',
  description: 'Description of the action group',
  actionGroupExecutor: {
    lambda: lambdaFunction.functionArn
  },
  actionGroupState: 'ENABLED',
  apiSchema: {
    payload: JSON.stringify({
      // OpenAPI 3.0 specification
    })
  }
}
```

### Agent Alias Creation
```typescript
const agentAlias = new bedrock.CfnAgentAlias(this, 'CertificacionesAgentAlias', {
  agentId: certificacionesAgent.attrAgentId,
  agentAliasName: 'production-alias',
  description: 'Production alias for certificaciones agent',
  routingConfiguration: [
    {
      agentVersion: 'DRAFT'
    }
  ]
});
```

### Lambda Permissions for Bedrock
```typescript
// Grant Bedrock permission to invoke Lambda functions
validarIdentidadFunction.addPermission('BedrockInvokePermission', {
  principal: new iam.ServicePrincipal('bedrock.amazonaws.com'),
  sourceArn: certificacionesAgent.attrAgentArn
});

// Repeat for all other Lambda functions
```

## Complete Stack Example
```typescript
export class BedrockAgentStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Import existing Lambda functions
    const validarIdentidadFunction = lambda.Function.fromFunctionArn(
      this, 'ValidarIdentidadFunction',
      'arn:aws:lambda:us-east-1:081899001252:function:cat-prod-lambda-validar-identidad'
    );

    // ... import other functions

    // Create agent role
    const agentRole = new iam.Role(this, 'BedrockAgentRole', {
      // ... role configuration
    });

    // Create agent
    const certificacionesAgent = new bedrock.CfnAgent(this, 'CertificacionesAgent', {
      // ... agent configuration
    });

    // Create alias
    const agentAlias = new bedrock.CfnAgentAlias(this, 'CertificacionesAgentAlias', {
      // ... alias configuration
    });

    // Grant permissions
    validarIdentidadFunction.addPermission('BedrockInvokePermission', {
      // ... permission configuration
    });
  }
}
```

## Notes

- The agent uses Claude 3.5 Sonnet as the foundation model
- All action groups are connected to specific Lambda functions
- The agent has a comprehensive instruction set for handling certification requests
- Multiple aliases exist for different versions/environments
- Each action group has its own OpenAPI schema definition
- The agent follows a strict sequential workflow for certification requests
- Session timeout is set to 10 minutes
- The agent is configured to only respond in Colombian Spanish

## Production Considerations

- Consider using versioned aliases instead of DRAFT for production
- Implement proper error handling and monitoring
- Set up CloudWatch logs for agent interactions
- Consider implementing rate limiting
- Ensure proper security policies for the agent role
- Test all action groups thoroughly before deployment
