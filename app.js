import { app, errorHandler } from 'mu';
import { getDuplicateIdentificators, reconciliatePerson } from './support';

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

app.use(errorHandler);
