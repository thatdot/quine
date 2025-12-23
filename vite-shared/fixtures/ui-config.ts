export const sampleQueries = [
  {
    name: "Get a few recent nodes",
    query: "CALL recentNodes(10)",
  },
  {
    name: "Get nodes by their ID(s)",
    query: "MATCH (n) WHERE id(n) = idFrom(0) RETURN n",
  },
];

export const nodeAppearances = [
  {
    predicate: {
      propertyKeys: [],
      knownValues: {},
      dbLabel: "Person",
    },
    icon: "",
    label: {
      key: "name",
      type: "Property",
    },
  },
  {
    predicate: {
      propertyKeys: [],
      knownValues: {},
      dbLabel: "File",
    },
    icon: "",
    label: {
      key: "path",
      prefix: "File path: ",
      type: "Property",
    },
  },
];

export const quickQueries = [
  {
    predicate: {
      propertyKeys: [],
      knownValues: {},
    },
    quickQuery: {
      name: "Adjacent Nodes",
      querySuffix: "MATCH (n)--(m) RETURN DISTINCT m",
      sort: {
        type: "Node",
      },
    },
  },
  {
    predicate: {
      propertyKeys: [],
      knownValues: {},
    },
    quickQuery: {
      name: "Refresh",
      querySuffix: "RETURN n",
      sort: {
        type: "Node",
      },
    },
  },
  {
    predicate: {
      propertyKeys: [],
      knownValues: {},
    },
    quickQuery: {
      name: "Local Properties",
      querySuffix: "RETURN id(n), properties(n)",
      sort: {
        type: "Text",
      },
    },
  },
];
