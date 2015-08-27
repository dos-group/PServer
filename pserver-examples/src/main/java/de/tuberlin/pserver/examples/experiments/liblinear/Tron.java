package de.tuberlin.pserver.examples.experiments.liblinear;


public class Tron {


/*
class Tron(val function : TronFunction)
{
	private def trcg(dataPoints : RDD[DataPoint], param : Parameter, delta : Double,  w_broad : Broadcast[DoubleMatrix], g : DoubleMatrix) : (Int, DoubleMatrix, DoubleMatrix) =
	{
		val n = w_broad.value.length
		var s = DoubleMatrix.zeros(n)
		var r = g.neg()
		var d = r.dup()
		var (rTr, rnewTrnew, beta, cgtol) = (0.0, 0.0, 0.0, 0.0)
		cgtol = 0.1 * g.norm2()

		var cgIter = 0
		rTr = r.dot(r)
		breakable {
            while(true)
            {
                if(r.norm2() <= cgtol)
                {
                    break()
                }
                cgIter += 1

                var Hd = function.hessianVector(dataPoints, w_broad, param, d)
                var alpha = rTr / d.dot(Hd)
                s.addi(d.mul(alpha))
                if(s.norm2() > delta)
                {
                    println("cg reaches trust region boundary")
                    alpha = -alpha
                    s.addi(d.mul(alpha))
                    val std = s.dot(d)
                    val sts = s.dot(s)
                    val dtd = d.dot(d)
                    val dsq = delta*delta
                    val rad = math.sqrt(std*std + dtd*(dsq-sts))
                    if (std >= 0)
                    {
                        alpha = (dsq - sts)/(std + rad)
                    }
                    else
                    {
                        alpha = (rad - std)/dtd
                    }
                    s.addi(d.mul(alpha))
                    alpha = -alpha
                    r.addi(Hd.mul(alpha))
                    break()
                }
                alpha = -alpha;
                r.addi(Hd.mul(alpha))
                rnewTrnew = r.dot(r)
                beta = rnewTrnew/rTr
                d.muli(beta)
                d.addi(r)
                rTr = rnewTrnew
            }
        }
    (cgIter, s, r)
    }

    def tron(prob : Problem, param : Parameter, eps : Double) : DoubleMatrix =
    {
        val ITERATIONS = 1000
        val (eta0, eta1, eta2) = (1e-4, 0.25, 0.75)
        val (sigma1, sigma2, sigma3) = (0.25, 0.5, 4.0)
        var (delta, snorm) = (0.0, 0.0)
        var (alpha, f, fnew, prered, actred, gs) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        var (search, iter) = (1, 1)
        var w = DoubleMatrix.zeros(prob.n)
        var w_new : DoubleMatrix = null
        var dataPoints = prob.dataPoints

        val sc = dataPoints.sparkContext
        var w_broad = sc.broadcast(w)
        f = function.functionValue(dataPoints, w_broad, param)

        var g = function.gradient(dataPoints, w_broad, param)
        delta = g.norm2()
        var gnorm1 = delta
        var gnorm = gnorm1
        if(gnorm <= eps * gnorm1)
        {
            search = 0
        }

        breakable {
            while(iter <= ITERATIONS && search == 1)
            {
                var (cgIter, s, r) = trcg(dataPoints, param, delta, w_broad, g)
                w_new = w.add(s)
                gs = g.dot(s)
                prered = -0.5*(gs - s.dot(r))
                w_broad.unpersist()
                w_broad = sc.broadcast(w_new)
                fnew = function.functionValue(dataPoints, w_broad, param)

                actred = f - fnew

                snorm = s.norm2()
                if (iter == 1)
                {
                    delta = math.min(delta, snorm)
                }

                if(fnew - f - gs <= 0)
                {
                    alpha = sigma3
                }
                else
                {
                    alpha = math.max(sigma1, -0.5*(gs/(fnew - f - gs)))
                }

                if (actred < eta0*prered)
                {
                    delta = math.min(math.max(alpha, sigma1)*snorm, sigma2*delta);
                }
                else if(actred < eta1*prered)
                {
                    delta = math.max(sigma1*delta, math.min(alpha*snorm, sigma2*delta))
                }
                else if (actred < eta2*prered)
                {
                    delta = math.max(sigma1*delta, math.min(alpha*snorm, sigma3*delta))
                }
                else
                {
                    delta = math.max(delta, math.min(alpha*snorm, sigma3*delta))
                }

                println("iter %2d act %5.3e pre %5.3e delta %5.3e f %5.3e |g| %5.3e CG %3d".format(iter, actred, prered, delta, f, gnorm, cgIter))

                if (actred > eta0*prered)
                {
                    iter += 1
                    w = w_new
                    f = fnew
                    g = function.gradient(dataPoints, w_broad, param)

                    gnorm = g.norm2()
                    if (gnorm <= eps*gnorm1)
                    {
                        break()
                    }
                }
                if (f < -1.0e+32)
                {
                    println("WARNING: f < -1.0e+32")
                    break()
                }
                if (math.abs(actred) <= 0 && prered <= 0)
                {
                    println("WARNING: actred and prered <= 0")
                    break()
                }
                if (math.abs(actred) <= 1.0e-12*math.abs(f) && math.abs(prered) <= 1.0e-12*math.abs(f))
                {
                    println("WARNING: actred and prered too small")
                    break()
                }
            }
        }
    w
    }
}
*/
}
