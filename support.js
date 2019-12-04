import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';

/**
 * Reconciliates duplicates of a person by making sure there is a unique person,
 * identifier and birthdate related to one RRN.
 *
 * First, all duplicates - based on the RRN - of a person, identifier and birthdate
 * are retrieved across graphs. These duplicates are called 'slaves'.
 *
 * Out of the slaves, a master record is constructed. The master record is as complete
 * as possible because it combines all properties of the several slaves. If a property
 * has different values across slaves, the first one found is selected as value for
 * the master record.
 *
 * Finally, all the slaves are removed in their respective graphs and replaced by
 * a copy of the master record. A reference from the slave to the master record is
 * kept using owl:sameAs.
 *
 * @public
 * @param rrn {string} RRN to reconciliate the duplicates for
 * @param options {Object} Options for execution
 * @param options.isDryRun {boolean} Whether to run the execution in test mode,
 *          only calculating the master record, but not execution INSERT/DELETE queries
*/
async function reconciliatePerson(rrn, options = {}) {
  const persons = await getDuplicatePersonUris(rrn);

  if (persons.length > 1) {
    console.log(`Found ${persons.length} duplicates of person with RRN ${rrn}`);

    const slaves = [];
    for (let { graph, uri } of persons) {
      const slave = await getPerson(graph, uri);
      slaves.push(slave);
    }
    const master = constructMaster(slaves);

    if (options.isDryRun) {
      console.log(`Constructed master record`);
      console.log(`Person: ${JSON.stringify(master.person)}`);
      console.log(`Identifier: ${JSON.stringify(master.identifier)}`);
      console.log(`Birthdate: ${JSON.stringify(master.birthdate)}`);
    } else {
      for (let slave of slaves) {
        await replaceSlaveWithMaster(slave, master);
      }
    }

  } else {
    console.log(`No duplicates found for person with RRN ${rrn}`);
  }
}

/**
 * Get all duplicate RRNs across graphs.
 *
 * An RRN is considered duplicate if persons with a different URI
 * are related to identifiers with the same RRN (skos:notation).
 *
 * @public
 * @return {Array} Array of RRNs as string
*/
async function getDuplicateIdentificators() {
  const result = await query(`
PREFIX person: <http://www.w3.org/ns/person#>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

SELECT DISTINCT ?rrn WHERE {
  GRAPH ?g {
    ?person a person:Person ;
    mu:uuid ?uuid ;
    adms:identifier ?identifier .

    ?identifier skos:notation ?rrn .
  }
  GRAPH ?h {
    ?identifier2 skos:notation ?rrn .

    ?person2 a person:Person ;
    mu:uuid ?uuid2 ;
    adms:identifier ?identifier2 .

  }
  FILTER (?person != ?person2)
}
`);

  return result.results.bindings.map(b => b['rrn'].value);
}

/**
 * Get all duplicate person URIs and the graph they reside in for a given RRN.
 *
 * @private
 * @return {Array} Array of objects with a person and a graph
*/
async function getDuplicatePersonUris(rrn) {
  const result = await query(`
PREFIX person: <http://www.w3.org/ns/person#>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

SELECT DISTINCT ?g ?person WHERE {
  GRAPH ?g {
    ?person a person:Person ;
    adms:identifier ?identifier .

    ?identifier skos:notation ${sparqlEscapeString(rrn)} .
  }
  GRAPH ?h {
    ?identifier2 skos:notation ${sparqlEscapeString(rrn)} .

    ?person2 a person:Person ;
    adms:identifier ?identifier2 .
  }
  FILTER (?person != ?person2)
}
`);

  return result.results.bindings.map(b => {
    const graph = b['g'].value;
    const uri = b['person'].value;
    return { graph,  uri };
  });
}

/**
 * Get a person, identifier and birthdate resource for a given person URI and graph.
 *
 * @private
 * @param graph {string} Graph the resources are stored in
 * @param uri {string} URI of the person
 * @return {Object} Object containing the graph, person, identifier and birthdate
*/
async function getPerson(graph, uri) {
  const result = await query(`
PREFIX person: <http://www.w3.org/ns/person#>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX persoon: <http://data.vlaanderen.be/ns/persoon#>

SELECT ?g ?uuid ?identifier ?familyName ?name ?firstName ?birthdate ?geboorteUuid ?date ?identifierUuid ?gender ?notation WHERE {
  GRAPH ${sparqlEscapeUri(graph)} {
    ${sparqlEscapeUri(uri)} a person:Person ;
      mu:uuid ?uuid ;
      adms:identifier ?identifier .

    OPTIONAL { ${sparqlEscapeUri(uri)} foaf:familyName ?familyName . }
    OPTIONAL { ${sparqlEscapeUri(uri)} foaf:name ?name . }
    OPTIONAL { ${sparqlEscapeUri(uri)} persoon:gebruikteVoornaam ?firstName . }

    OPTIONAL {
      ${sparqlEscapeUri(uri)} persoon:heeftGeboorte ?birthdate .
      OPTIONAL { ?birthdate mu:uuid ?geboorteUuid . }
      OPTIONAL { ?birthdate persoon:datum ?date . }
    }

    OPTIONAL { ?identifier mu:uuid ?identifierUuid . }
    OPTIONAL { ?identifier skos:notation ?notation . }

    OPTIONAL { ${sparqlEscapeUri(uri)} persoon:geslacht ?gender . }
  }
}
`);

  if (result.results.bindings.length > 1) {
    console.warn(`${result.results.bindings.length} matches found for person <${uri}>. Probably some multi-value properties? Only taking the first result into account.`);
    const bindingKeys = ['uuid', 'familyName', 'name', 'firstName', 'geboorteUuid', 'date', 'identifierUuid', 'gender', 'notation'];
    for (let key of bindingKeys) {
      const values = result.results.bindings.map(b => b[key] && b[key].value);
      console.log(`Values for prop '${key}': ${values}`);
    }
  }

  if (result.results.bindings.length) {
    const binding = result.results.bindings[0];

    // Construct person resource
    const person = {
      uri: uri,
      uuid: binding['uuid'].value,
      identifier: binding['identifier'].value
    };
    const personProps = ['familyName', 'name', 'firstName', 'gender', 'birthdate'];
    addOptionalProperties(person, binding, personProps);

    // Construct identifier resource
    const identifier = {
      uri: binding['identifier'].value
    };
    const identifierProps = [['identifierUuid', 'uuid'], 'notation'];
    addOptionalProperties(identifier, binding, identifierProps);

    // Construct birthdate resource
    let birthdate = null;
    if (binding['birthdate']) {
      birthdate = {
        uri: binding['birthdate'].value
      };
    }
    const birthdateProps = [['geboorteUuid', 'uuid'], 'date'];
    addOptionalProperties(birthdate, binding, birthdateProps);

    return { graph, person, identifier, birthdate };
  } else {
    return null;
  }
}

/**
 * Sets a set of optional values of the binding result set as
 * properties on a given resource.
 *
 * If a property is passed as an array, the first element is
 * used as key in the binding result set, while the second element
 * is used as key of the property for the resource.
 *
 * @private
 * @param resource {Object} Resource to set the properties on
 * @param binding {Object} Binding result set of a SPARQL query
 * @param props {Array} Array of properties to set on the resource
*/
function addOptionalProperties(resource, binding, props) {

  /** Set a given key from the binding result set as prop on the given resource */
  function addOptionalPropertyAs(resource, binding, prop, key) {
    if (!key)
      key = prop;

    if (binding[prop])
      resource[key] = binding[prop].value;
  }


  for (let prop of props) {
    if (typeof(prop) == 'string') {
      addOptionalPropertyAs(resource, binding, prop);
    } else {
      addOptionalPropertyAs(resource, binding, prop[0], prop[1]);
    }
  }
}

/**
 * Construct a master record out of a set of slave records.
 *
 * The master record is as complete  as possible because it combines all properties
 * of the several slaves. If a property has different values across slaves,
 * the first one found is selected as value for the master record.
 *
 * @private
 * @param slaves {Array} Array of slave objects, each containing a person, identifier and birthdate.
 * @return {Object} Master recording consisting of a person, identifier and birthdate resource.
*/
function constructMaster(slaves) {
  function constructMasterForResource(resources, props) {
    const slaves = resources.filter(r => r);
    const master = {};

    // for each property, walk over the slaves until we find a value. That value will become the master's value
    for (let prop of props) {
      const slave = slaves.find(s => s[prop]);
      if (slave)
        master[prop] = slave[prop];
    }

    if (Object.keys(master).length)
      return master;
    else
      return null;
  }

  const person = constructMasterForResource(slaves.map(r => r.person), ['uri', 'uuid', 'familyName', 'name', 'firstName', 'gender']);
  const identifier = constructMasterForResource(slaves.map(r => r.identifier), ['uri', 'uuid', 'notation']);
  const birthdate = constructMasterForResource(slaves.map(r => r.birthdate), ['uri', 'uuid', 'date']);

  if (person) {
    person['identifier'] = identifier && identifier.uri;
    person['birthdate'] = birthdate && birthdate.uri;
  }

  return { person, identifier, birthdate };
}

/**
 * Replace a slave with a copy of the master.
 * A reference from the slave URI to the master record is kept using owl:sameAs.
 *
 * @private
 * @param graph {string} Graph the resources are stored in
 * @param uri {string} URI of the person
*/
async function replaceSlaveWithMaster(slave, master) {
  const graph = slave.graph;

  await deleteSlaveData(slave.person.uri, graph);
  await insertMasterData(master, graph);

  if (slave.person) {
    await replaceSlaveUris(master.person.uri, slave.person.uri, graph);
    await insertSameAs(master.person.uri, slave.person.uri, graph);
  }

  if (slave.identifier) {
    await replaceSlaveUris(master.identifier.uri, slave.identifier.uri, graph);
    await insertSameAs(master.identifier.uri, slave.identifier.uri, graph);
  }

  if (slave.birthdate) {
    await replaceSlaveUris(master.birthdate.uri, slave.birthdate.uri, graph);
    await insertSameAs(master.birthdate.uri, slave.birthdate.uri, graph);
  }
}

/**
 * Delete the data of a slave, specified by its person URI, from a specific graph.
 * The person, identifier and birthdate are all deleted.
 *
 * @private
 * @param graph {string} Graph the resources are stored in
 * @param uri {string} URI of the person
*/
async function deleteSlaveData(uri, graph) {
  await update(`
PREFIX person: <http://www.w3.org/ns/person#>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX persoon: <http://data.vlaanderen.be/ns/persoon#>

DELETE {
  ${sparqlEscapeUri(uri)} a person:Person ;
      mu:uuid ?uuid ;
      adms:identifier ?identifier ;
      foaf:familyName ?familyName ;
      foaf:name ?name ;
      persoon:gebruikteVoornaam ?firstName ;
      persoon:heeftGeboorte ?birthdate ;
      persoon:geslacht ?gender .

  ?birthdate a persoon:Geboorte ;
      mu:uuid ?geboorteUuid ;
      persoon:datum ?date .

  ?identifier a adms:Identifier ;
      mu:uuid ?identifierUuid ;
      skos:notation ?notation .

} WHERE {
  GRAPH ${sparqlEscapeUri(graph)} {
    ${sparqlEscapeUri(uri)} a person:Person ;
      mu:uuid ?uuid ;
      adms:identifier ?identifier .

    OPTIONAL { ${sparqlEscapeUri(uri)} foaf:familyName ?familyName . }
    OPTIONAL { ${sparqlEscapeUri(uri)} foaf:name ?name . }
    OPTIONAL { ${sparqlEscapeUri(uri)} persoon:gebruikteVoornaam ?firstName . }

    OPTIONAL {
      ${sparqlEscapeUri(uri)} persoon:heeftGeboorte ?birthdate .
      OPTIONAL { ?birthdate a persoon:Geboorte . }
      OPTIONAL { ?birthdate mu:uuid ?geboorteUuid . }
      OPTIONAL { ?birthdate persoon:datum ?date . }
    }

    OPTIONAL { ?identifier a adms:Identifier . }
    OPTIONAL { ?identifier mu:uuid ?identifierUuid . }
    OPTIONAL { ?identifier skos:notation ?notation . }

    OPTIONAL { ${sparqlEscapeUri(uri)} persoon:geslacht ?gender . }
  }
}
`);
}

/**
 * Insert the master data in a specific graph. The master data consists of
 * a person, identifier and birthdate resource.
 *
 * @private
 * @param master {Object} Data to insert
 * @param graph {string} Graph to insert the data in
*/
async function insertMasterData(master, graph) {
  const statements = [];

  const personUri = sparqlEscapeUri(master.person.uri);
  statements.push(`${personUri} a person:Person .`);
  statements.push(`${personUri} mu:uuid ${sparqlEscapeString(master.person.uuid)} .`);
  if (master.person.familyName) statements.push(`${personUri} foaf:familyName ${sparqlEscapeString(master.person.familyName)} .`);
  if (master.person.name) statements.push(`${personUri} foaf:name ${sparqlEscapeString(master.person.name)} .`);
  if (master.person.firstName) statements.push(`${personUri} persoon:gebruikteVoornaam ${sparqlEscapeString(master.person.firstName)} .`);
  if (master.person.identifier) statements.push(`${personUri} adms:identifier ${sparqlEscapeUri(master.person.identifier)} .`);
  if (master.person.birthdate) statements.push(`${personUri} persoon:heeftGeboorte ${sparqlEscapeUri(master.person.birthdate)} .`);
  if (master.person.gender) statements.push(`${personUri} persoon:geslacht ${sparqlEscapeUri(master.person.gender)} .`);

  if (master.identifier) {
    const identifierUri = sparqlEscapeUri(master.identifier.uri);
    statements.push(`${identifierUri} a adms:Identifier .`);
    if (master.identifier.uuid) statements.push(`${identifierUri} mu:uuid ${sparqlEscapeString(master.identifier.uuid)} .`);
    if (master.identifier.notation) statements.push(`${identifierUri} skos:notation ${sparqlEscapeString(master.identifier.notation)} .`);
  }

  if (master.birthdate) {
    const birthdateUri = sparqlEscapeUri(master.birthdate.uri);
    statements.push(`${birthdateUri} a persoon:Geboorte .`);
    if (master.birthdate.uuid) statements.push(`${birthdateUri} mu:uuid ${sparqlEscapeString(master.birthdate.uuid)} .`);
    if (master.birthdate.date) statements.push(`${birthdateUri} persoon:datum ${sparqlEscapeString(master.birthdate.date)}^^xsd:date .`);
  }

  return await update(`
PREFIX person: <http://www.w3.org/ns/person#>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX persoon: <http://data.vlaanderen.be/ns/persoon#>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(graph)} {
        ${statements.join('\n')}
      }
    }
  `);
}

/**
 * Replace all occurrences of a slave URI with the master URI in a given graph
 *
 * @private
 * @param masterUri {string} URI of the master to insert
 * @param slaveUri {string} URI of the slave to delete
 * @param graph {string} Graph to insert in and delete from
*/
async function replaceSlaveUris(masterUri, slaveUri, graph) {
  await update(`
    DELETE {
      GRAPH ${sparqlEscapeUri(graph)} {
        ${sparqlEscapeUri(slaveUri)} ?p ?o .
      }
    }
    INSERT {
      GRAPH ${sparqlEscapeUri(graph)} {
        ${sparqlEscapeUri(masterUri)} ?p ?o .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(graph)} {
        ${sparqlEscapeUri(slaveUri)} ?p ?o .
      }
    }
  `);

  await update(`
    DELETE {
      GRAPH ${sparqlEscapeUri(graph)} {
        ?s ?p ${sparqlEscapeUri(slaveUri)} .
      }
    }
    INSERT {
      GRAPH ${sparqlEscapeUri(graph)} {
        ?s ?p ${sparqlEscapeUri(masterUri)} .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(graph)} {
        ?s ?p ${sparqlEscapeUri(slaveUri)} .
      }
    }
  `);
}

/**
 * Insert a reference from the slave URI to the master URI
 * in a given graph using owl:sameAs.
 *
 * @private
 * @param masterUri {string} URI of the master to reference
 * @param slaveUri {string} URI of the slave to be referenced
 * @param graph {string} Graph to insert the data in
*/
async function insertSameAs(masterUri, slaveUri, graph) {
  await update(`
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(graph)} {
        ${sparqlEscapeUri(slaveUri)} owl:sameAs ${sparqlEscapeUri(masterUri)} .
      }
    }
  `);
}

export {
  reconciliatePerson,
  getDuplicateIdentificators
}
