package si.pele.concurrent.leftright;

/**
 * @author peter
 */
public interface EnterExitWait {
    void enter();
    void exit();
    void waitEmpty();
}
