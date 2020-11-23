import { app, errorHandler } from 'mu';
import { getDuplicateIdentificators, reconciliatePerson, getRrn } from './support';
import bodyParser from 'body-parser';
import flatten from 'lodash.flatten';
import { CronJob } from 'cron';
import request from 'request';

const cronFrequency = process.env.RECONCILIATION_CRON_PATTERN || '0 0 1 * * *';
new CronJob(cronFrequency, function() {
  console.log(`Reconciliation triggered by cron job at ${new Date().toISOString()}`);
  request.post('http://localhost/reconciliate/');
}, null, true);

app.use(bodyParser.json({ type: function(req) { return /^application\/json/.test(req.get('content-type')); } }));

app.get('/report', async function(req, res, next) {
  try {
    const rrns = await getDuplicateIdentificators();
    res.status(200).send({ duplicates: rrns.length });
  }
  catch(e) {
    console.error(e);
    next(new Error(e.message));
  }
});

app.post('/reconciliate', async function(req, res, next) {
  const isDryRun = req.query['dry-run'];

  try {
    const rrns = await getDuplicateIdentificators();
    console.log(`Found ${rrns.length} duplicate RRNs`);

    async function reconciliateBulk(rrns) {
      const total = rrns.length;
      let i = 1;
      for (let rrn of rrns) {
        console.log(`Reconciliating ${i}/${total}`);
        await reconciliatePerson(rrn, { isDryRun });
        i++;
      }
    }

    reconciliateBulk(rrns); // don't await, but execute in background

    res.status(202).end();
  }
  catch(e) {
    console.error(e);
    next(new Error(e.message));
  }
});

app.post('/reconciliate/:rrn', async function(req, res, next) {
  const rrn = req.params.rrn;
  const isDryRun = req.query['dry-run'];
  try {
    await reconciliatePerson(rrn, { isDryRun });
    res.status(204).end();
  }
  catch(e) {
    console.error(e);
    next(new Error(e.message));
  }
});

app.post('/delta', async function(req, res, next) {
  const identificators = getIdentificators(req.body);
  if (!identificators.length) {
    console.log("Deltas do not contain an identificator. Nothing should happen.");
    return res.status(204).send();
  }

  for (let identificator of identificators) {
    try {
      const rrn = await getRrn(identificator);
      await reconciliatePerson(rrn);
    } catch (e) {
      console.log(`Something went wrong while handling deltas for identificator ${identificator}`);
      console.log(e);
      return next(e);
    }
  }

  return res.status(202).send();
});

/**
 * Returns the identificator URIs found in the deltas.
 *
 * @param Object delta Message as received from the delta notifier
*/
function getIdentificators(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  return inserts.filter(isIdentificatorTriple).map(t => t.object.value);
}

/**
 * Returns whether the passed triple is an addition of identificator to a person or not.
 *
 * @param Object triple Triple as received from the delta notifier
*/
function isIdentificatorTriple(triple) {
  return triple.predicate.value == 'http://www.w3.org/ns/adms#identifier';
};

app.use(errorHandler);
