import sys
import numpy as np
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel
from PyQt5.QtCore import Qt

class TriStateButton(QPushButton):
    def __init__(self, state_counter, state_counter_labels, state_button_labels, label_text, parent=None):
        super().__init__(parent)
        self.dotSize = 15
        self.setFixedSize(self.dotSize, self.dotSize)
        self.state = 0  # 0: default, 1: orange with '?', 2: red with 'X'
        self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #3498db; color: white;")
        self.clicked.connect(self.changeState)
        self.state_counter = state_counter
        self.state_counter_labels = state_counter_labels
        self.state_button_labels =  state_button_labels
        self.label_text = label_text
        # self.state_buttons = {0: [], 1: [], 2: []}  # Store buttons in each state

    def changeState(self):
        old_state = self.state
        self.state = (self.state + 1) % 3
        self.state_counter[old_state] -= 1
        self.state_counter[self.state] += 1
        self.updateCounter()
        if self.state == 0:
            self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #3498db; color: white;")
            self.setText("")
        elif self.state == 1:
            self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #e67e22; color: white;")
            self.setText("?")
        elif self.state == 2:
            self.setStyleSheet(f"border-radius: {int(self.dotSize/2)}px; background-color: #e74c3c; color: white;")
            self.setText("X")
        self.setFixedSize(self.dotSize, self.dotSize)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        radius = min(self.width(), self.height()) // 2
        self.setStyleSheet(f"border-radius: {radius}px; background-color: {self.palette().button().color().name()}; color: white;")

    def updateCounter(self):
        altlab = ['nom','redo','bad']        
        for state, count_label in self.state_counter_labels.items():
            count_label.setText(f"{altlab[state]}: {self.state_counter[state]}")

       # Update labels of buttons in each state
        for state , count_label in self.state_counter_labels.items():
            buttons_in_state = [button.label_text for button in self.parent().findChildren(TriStateButton) if button.state == state]
            # print(buttons_in_state)
            self.state_button_labels[state].setText(f"{altlab[state]}: {' '.join(buttons_in_state)}")

        # # Update labels of buttons in each state
        # for state, count_label in self.state_counter_labels.items():
        #     buttons_in_state = [button.text() for button in self.parent().findChildren(TriStateButton) if button.state == state]
        #     self.state_button_labels[state].setText(f"{altlab[state]}: {' '.join(buttons_in_state)}")

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Tri-State Buttons")
        self.setGeometry(100, 100, 600, 600)
        center_x = self.width() / 2
        center_y = self.height() / 2

        # button_labels = ['a', 'b', 'c', 'd', 'e']
        # positions = [[0.1, 0.3], [0.6, 0.2], [0.6, 0.8], [0.3, 0.5], [0.9, 0.5]]
        # np.random.seed(10); n = 10; 
        # positions = np.array([np.random.rand(n),np.random.rand(n)]).transpose()
        # button_labels = np.arange(50).astype(str)
        # self.state_counter = {0: n, 1: 0, 2: 0}

        s = 6
        positions = np.array([[r*np.cos(2*np.pi*i/s),r*np.sin(2*np.pi*i/s)] for i in range(s) for r in [0.15, 0.2,0.35,0.43]])
        button_labels = np.arange(len(positions)).astype(str)

        self.state_counter = {0: len(positions), 1: 0, 2: 0}
        self.state_counter_labels = {}
        self.state_button_labels = {}

        altlab = ['nom','redo','bad']
        for state in self.state_counter:
            lab = QLabel(f"{altlab[state]}: {self.state_counter[state]}", self)
            lab.move(20, 0 + state * 20)
            self.state_counter_labels[state] = lab

        for state in self.state_counter:
            label = QLabel(f"{altlab[state]}: ", self)
            label.move(20, 80 + state * 20)
            self.state_button_labels[state] = label

        for pos, label_text in zip(positions, button_labels):
            button = TriStateButton(self.state_counter, self.state_counter_labels, self.state_button_labels, label_text, self)
            x = int(pos[0] * self.width() + center_x - button.width()/2)
            y = int(pos[1] * self.height() + center_y - button.height()/2)
            button.move(x, y)

             # Create label for the button
            label = QLabel(label_text, self)
            label.move(x + button.width() + 2, y + button.height())
            label.setAlignment(Qt.AlignVCenter)
            label.setStyleSheet("font-size: 16px;")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    mainWindow = MainWindow()
    mainWindow.show()
    sys.exit(app.exec_())

