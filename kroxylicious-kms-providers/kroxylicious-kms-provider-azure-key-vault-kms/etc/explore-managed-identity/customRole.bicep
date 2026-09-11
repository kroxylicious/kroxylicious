//
// Copyright Kroxylicious Authors.
//
// Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
//

// This module is intended for subscription-level deployments. It is for developer use-cases and exploratory testing
targetScope = 'subscription'

@description('The unique name for the custom role.')
param customRoleName string

resource customRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' = {
  name: guid(subscription().id, customRoleName)
  properties: {
    roleName: customRoleName
    description: 'Allows getting key metadata and performing wrap/unwrap operations only.'
    type: 'CustomRole'
    permissions: [
      {
        dataActions: [
          'Microsoft.KeyVault/vaults/keys/read'
          'Microsoft.KeyVault/vaults/keys/wrap/action'
          'Microsoft.KeyVault/vaults/keys/unwrap/action'
        ]
        notDataActions: []
      }
    ]
    assignableScopes: [
      subscription().id
    ]
  }
}

// Output the new role's ID so the main template can use it for the assignment.
output customRoleId string = customRole.id
