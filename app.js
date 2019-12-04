import { app, errorHandler } from 'mu';
import { getDuplicateIdentificators, reconsiliatePerson } from './support';

app.post('/reconsiliate', async function(req, res, next) {
  const isDryRun = req.query['dry-run'];

  try {
    const rrns = await getDuplicateIdentificators();
    console.log(`Found ${rrns.length} duplicate RRNs`);

    async function reconsiliateBulk(rrns) {
      const total = rrns.length;
      let i = 1;
      for (let rrn of rrns) {
        console.log(`Reconsiliating ${i}/${total}`);
        await reconsiliatePerson(rrn, { isDryRun });
        i++;
      }
    }

    reconsiliateBulk(rrns); // don't await, but execute in background

    res.status(202).end();
  }
  catch(e) {
    console.error(e);
    next(new Error(e.message));
  }
});

app.post('/reconsiliate/:rrn', async function(req, res, next) {
  const rrn = req.params.rrn;
  const isDryRun = req.query['dry-run'];
  try {
    await reconsiliatePerson(rrn, { isDryRun });
    res.status(204).end();
  }
  catch(e) {
    console.error(e);
    next(new Error(e.message));
  }
});

app.use(errorHandler);
