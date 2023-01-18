/* eslint-disable @typescript-eslint/naming-convention */
import type { TypedDocumentNode as DocumentNode } from '@graphql-typed-document-node/core'
import * as Types from '../../../../model/graphql/types.generated'

import {
  RoleSelection_ManagedRole_Fragment,
  RoleSelection_UnmanagedRole_Fragment,
  RoleSelectionFragmentDoc,
} from './RoleSelection.generated'

export type containers_Admin_RolesAndPolicies_gql_RoleCreateUnmanagedMutationVariables =
  Types.Exact<{
    input: Types.UnmanagedRoleInput
  }>

export type containers_Admin_RolesAndPolicies_gql_RoleCreateUnmanagedMutation = {
  readonly __typename: 'Mutation'
} & {
  readonly roleCreate:
    | ({ readonly __typename: 'RoleCreateSuccess' } & {
        readonly role:
          | ({ readonly __typename: 'ManagedRole' } & RoleSelection_ManagedRole_Fragment)
          | ({
              readonly __typename: 'UnmanagedRole'
            } & RoleSelection_UnmanagedRole_Fragment)
      })
    | { readonly __typename: 'RoleHasTooManyPoliciesToAttach' }
    | { readonly __typename: 'RoleNameExists' }
    | { readonly __typename: 'RoleNameInvalid' }
    | { readonly __typename: 'RoleNameReserved' }
}

export const containers_Admin_RolesAndPolicies_gql_RoleCreateUnmanagedDocument = {
  kind: 'Document',
  definitions: [
    {
      kind: 'OperationDefinition',
      operation: 'mutation',
      name: {
        kind: 'Name',
        value: 'containers_Admin_RolesAndPolicies_gql_RoleCreateUnmanaged',
      },
      variableDefinitions: [
        {
          kind: 'VariableDefinition',
          variable: { kind: 'Variable', name: { kind: 'Name', value: 'input' } },
          type: {
            kind: 'NonNullType',
            type: {
              kind: 'NamedType',
              name: { kind: 'Name', value: 'UnmanagedRoleInput' },
            },
          },
        },
      ],
      selectionSet: {
        kind: 'SelectionSet',
        selections: [
          {
            kind: 'Field',
            alias: { kind: 'Name', value: 'roleCreate' },
            name: { kind: 'Name', value: 'roleCreateUnmanaged' },
            arguments: [
              {
                kind: 'Argument',
                name: { kind: 'Name', value: 'input' },
                value: { kind: 'Variable', name: { kind: 'Name', value: 'input' } },
              },
            ],
            selectionSet: {
              kind: 'SelectionSet',
              selections: [
                {
                  kind: 'InlineFragment',
                  typeCondition: {
                    kind: 'NamedType',
                    name: { kind: 'Name', value: 'RoleCreateSuccess' },
                  },
                  selectionSet: {
                    kind: 'SelectionSet',
                    selections: [
                      {
                        kind: 'Field',
                        name: { kind: 'Name', value: 'role' },
                        selectionSet: {
                          kind: 'SelectionSet',
                          selections: [
                            {
                              kind: 'FragmentSpread',
                              name: { kind: 'Name', value: 'RoleSelection' },
                            },
                          ],
                        },
                      },
                    ],
                  },
                },
              ],
            },
          },
        ],
      },
    },
    ...RoleSelectionFragmentDoc.definitions,
  ],
} as unknown as DocumentNode<
  containers_Admin_RolesAndPolicies_gql_RoleCreateUnmanagedMutation,
  containers_Admin_RolesAndPolicies_gql_RoleCreateUnmanagedMutationVariables
>

export { containers_Admin_RolesAndPolicies_gql_RoleCreateUnmanagedDocument as default }
