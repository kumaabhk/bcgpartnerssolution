from context.local_context import Context
from context.culster_context import ClusterContext
from transforms.ratings import Ratings

#context= ClusterContext().spark # cluster context
context= Context().spark #local context

ratings = Ratings(context)
ratings.transform()

