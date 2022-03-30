import { RuleEngine } from '../../rule/rule-engine';
import { Processor } from '../builder'

export default Processor()
    .order(100)
    .handler(({logger, state, tracker, context }) => {

        const ruleEngine = new RuleEngine(context);

        return ruleEngine.execute(state, tracker);

    })
