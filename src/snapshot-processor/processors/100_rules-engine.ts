import { RuleEngine } from '../../rule/rule-engine';
import { Processor } from '../builder'

export default Processor()
    .order(100)
    .handler(({logger, state, tracker, rules, ruleEngineResult, context }) => {

        const ruleEngine = new RuleEngine(context);

        return ruleEngine.execute(state, rules, tracker)
            .then(result => {
                ruleEngineResult.rules = result.rules;
                ruleEngineResult.markers = result.markers;
            });
    })
