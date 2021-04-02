// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.utility.Parameters;

/**
 * Transforms an #inside operator into a #extents operator using the field
 * parts.
 *
 *
 * @author sjh
 */
public class InsideToFieldPartTraversal extends Traversal {

    protected Parameters availableParts;

    public InsideToFieldPartTraversal(Retrieval retrieval) throws IOException {
        this.availableParts = retrieval.getAvailableParts();
    }

    @Override
    public void beforeNode(Node original, Parameters qp) throws Exception {
    }

    @Override
    public Node afterNode(Node original, Parameters qp) throws Exception {
        if (original.getOperator().equals("inside")) {
            // a way out - in case you really want it.
            if (original.getNodeParameters().get("noOpt", false)) {
                return original;
            }

            List<Node> children = original.getInternalNodes();
            if (children.size() != 2) {
                return original;
            }
            Node text = children.get(0);
            Node field = children.get(1);

            // MCZ: if this is not an "extent" operator, just return the 
            // original query. For example, it could be a window operator
            // and the code below would fail. This code is trying to optimize 
            // the query, but if we have a window, we have to iterate over to
            // find it anyays so there's not much optimization that can be done.
            if (!text.getOperator().equals("extents")) {
                return original;
            }

            assert (text.getOperator().equals("extents")
                    && text.getNodeParameters().isString("part"));
            assert (field.getOperator().equals("extents")
                    && field.getNodeParameters().isString("part"));

            String fieldPart = text.getNodeParameters().getString("part").replaceFirst("postings", "field") + "." + field.getDefaultParameter();

            if (!availableParts.containsKey(fieldPart)) {
                return original;
            }

            Node n = TextPartAssigner.transformedNode(text.clone(), fieldPart);
            n.setOperator("extents");
            return n;
        } else {
            return original;
        }
    }
}
